"""Store implementations: the five SQL statements of ``docs/design.md`` §4.2.

All statements are parameterized and all timestamps are computed by the SDK
clock and bound as parameters -- no ``now()`` / ``CURRENT_TIMESTAMP`` in SQL.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterator, Protocol, Sequence, runtime_checkable
from urllib.parse import parse_qsl, unquote, urlsplit

logger = logging.getLogger(__name__)

SQLITE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"


class Dialect(str, Enum):
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SQLITE = "sqlite"

    def __str__(self) -> str:
        return self.value


class StoreError(Exception):
    """A database operation failed for a reason other than a duplicate key."""


class DuplicateKeyError(StoreError):
    """Q2 ``INSERT`` hit the ``task_execution_lock`` primary key."""


@runtime_checkable
class Store(Protocol):
    """Persistence port used by :class:`~startup_task.coordinator.Coordinator`."""

    dialect: Dialect

    def exists_success(self, task_id: str, version: str) -> bool:
        """Q1: is there a ``status='success'`` record for (task_id, version)?"""

    def insert_lock(
        self,
        task_id: str,
        owner_id: str,
        acquired_at: datetime,
        expires_at: datetime,
    ) -> None:
        """Q2: atomic ``INSERT``; raises :class:`DuplicateKeyError` if held."""

    def takeover_lock(
        self,
        owner_id: str,
        acquired_at: datetime,
        expires_at: datetime,
        task_id: str,
        now: datetime,
    ) -> int:
        """Q3: conditional ``UPDATE`` of an expired lock; returns affected rows."""

    def upsert_version(
        self,
        task_id: str,
        version: str,
        status: str,
        executed_at: datetime,
        owner_id: str,
    ) -> None:
        """Q4: upsert the version record."""

    def release_lock(self, task_id: str, owner_id: str) -> None:
        """Q5: delete the lock only when it is still owned by ``owner_id``."""

    def execute_ddl(self, script: str) -> None:
        """Run a DDL script (``ensure_schema``)."""


Q1_EXISTS_SUCCESS = (
    "SELECT 1 FROM task_version_record"
    " WHERE task_id = ? AND version = ? AND status = 'success'"
)

Q2_INSERT_LOCK = (
    "INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at)"
    " VALUES (?, ?, ?, ?)"
)

Q3_TAKEOVER_LOCK = (
    "UPDATE task_execution_lock"
    "   SET owner_id = ?, acquired_at = ?, expires_at = ?"
    " WHERE task_id = ? AND expires_at < ?"
)

Q4_UPSERT_VERSION_ON_CONFLICT = (
    "INSERT INTO task_version_record (task_id, version, status, executed_at, owner_id)"
    " VALUES (?, ?, ?, ?, ?)"
    " ON CONFLICT (task_id, version) DO UPDATE"
    "    SET status = excluded.status,"
    "        executed_at = excluded.executed_at,"
    "        owner_id = excluded.owner_id"
)

Q4_UPSERT_VERSION_ON_DUPLICATE_KEY = (
    "INSERT INTO task_version_record (task_id, version, status, executed_at, owner_id)"
    " VALUES (?, ?, ?, ?, ?)"
    " ON DUPLICATE KEY UPDATE"
    "     status = VALUES(status), executed_at = VALUES(executed_at),"
    "     owner_id = VALUES(owner_id)"
)

Q5_RELEASE_LOCK = "DELETE FROM task_execution_lock WHERE task_id = ? AND owner_id = ?"

#: Read-back used only where the driver's affected-row count is not the matched
#: row count (MySQL). Confirms that the row really carries our write values.
Q3_CONFIRM_TAKEOVER = (
    "SELECT owner_id, expires_at FROM task_execution_lock WHERE task_id = ?"
)


class SqlStore:
    """Shared logic for the three SQL dialects.

    Subclasses provide the DB-API connection and two dialect hooks:
    :meth:`_bind_time` (timestamp representation) and
    :meth:`_is_duplicate_key` (primary-key conflict detection).

    Transaction semantics: every statement is its own unit of work. The SDK
    never spans a multi-statement transaction -- Q2 and Q3 must be individually
    atomic so other pods observe the lock, and Q4/Q5 are single upsert/delete
    statements. When a caller injects a connection factory that returns
    ``autocommit=False`` connections (psycopg2's default), each successful
    statement is committed here and each failed statement is rolled back, so a
    failed Q2 does not leave PostgreSQL in the aborted state (SQLState 25P02)
    and break the Q3 takeover that follows it.
    """

    dialect: Dialect = Dialect.POSTGRES
    _paramstyle = "format"
    #: True when ``cursor.rowcount`` reports *matched* rows for Q3 (PG/SQLite).
    #: MySQL reports *changed* rows by default, so an identical re-takeover
    #: yields 0 and must be confirmed with :data:`Q3_CONFIRM_TAKEOVER`.
    _q3_rowcount_is_matched_rows = True

    def __init__(self) -> None:
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ hooks

    def _connect(self) -> Any:  # pragma: no cover - overridden
        raise NotImplementedError

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        """Yield a DB-API connection in autocommit mode."""
        raise NotImplementedError

    def _bind_time(self, value: datetime) -> Any:  # pragma: no cover - overridden
        raise NotImplementedError

    def _is_duplicate_key(self, exc: BaseException) -> bool:  # pragma: no cover
        raise NotImplementedError

    # --------------------------------------------------------------- plumbing

    def _sql(self, sql: str) -> str:
        if self._paramstyle == "qmark":
            return sql
        return sql.replace("?", "%s")

    def _execute(
        self,
        sql: str,
        params: Sequence[Any] = (),
        *,
        classify_duplicate: bool = False,
    ) -> int:
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(self._sql(sql), tuple(params))
                rowcount = cursor.rowcount
            except Exception as exc:  # noqa: BLE001 - classified below
                # A failed statement may leave an open transaction that blocks
                # every following statement on this connection; clear it before
                # the caller's conflict branch runs Q3.
                self._abort_statement(conn)
                if classify_duplicate and self._is_duplicate_key(exc):
                    raise DuplicateKeyError(f"lock already held: {exc}") from exc
                raise StoreError(f"query failed: {exc}") from exc
            finally:
                cursor.close()
            self._finish_statement(conn)
            return rowcount

    def _fetch_one(self, sql: str, params: Sequence[Any] = ()) -> tuple[Any, ...] | None:
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(self._sql(sql), tuple(params))
                row = cursor.fetchone()
            except Exception as exc:  # noqa: BLE001
                self._abort_statement(conn)
                raise StoreError(f"query failed: {exc}") from exc
            finally:
                cursor.close()
            self._finish_statement(conn)
            return row

    # --------------------------------------------------- transaction handling

    def _finish_statement(self, conn: Any) -> None:
        """Commit the statement when the connection is not in autocommit mode."""
        if self._in_transaction(conn):
            conn.commit()

    def _abort_statement(self, conn: Any) -> None:
        """Roll back after a failed statement so the connection stays usable."""
        if not self._in_transaction(conn):
            return
        try:
            conn.rollback()
        except Exception as exc:  # noqa: BLE001 - best effort cleanup
            logger.warning("rollback after failed statement failed: %s", exc)

    @staticmethod
    def _in_transaction(conn: Any) -> bool:
        get_autocommit = getattr(conn, "get_autocommit", None)
        if callable(get_autocommit):
            # pymysql: autocommit is toggled through methods, not an attribute.
            return not get_autocommit()
        if getattr(conn, "autocommit", True) is False:
            return True
        # sqlite3 with an explicit isolation level keeps an implicit transaction.
        return bool(getattr(conn, "in_transaction", False))

    # ----------------------------------------------------------------- the API

    def exists_success(self, task_id: str, version: str) -> bool:
        row = self._fetch_one(Q1_EXISTS_SUCCESS, (task_id, version))
        return row is not None

    def insert_lock(
        self,
        task_id: str,
        owner_id: str,
        acquired_at: datetime,
        expires_at: datetime,
    ) -> None:
        self._execute(
            Q2_INSERT_LOCK,
            (
                task_id,
                owner_id,
                self._bind_time(acquired_at),
                self._bind_time(expires_at),
            ),
            classify_duplicate=True,
        )

    def takeover_lock(
        self,
        owner_id: str,
        acquired_at: datetime,
        expires_at: datetime,
        task_id: str,
        now: datetime,
    ) -> int:
        rows = self._execute(
            Q3_TAKEOVER_LOCK,
            (
                owner_id,
                self._bind_time(acquired_at),
                self._bind_time(expires_at),
                task_id,
                self._bind_time(now),
            ),
        )
        if rows == 1 or self._q3_rowcount_is_matched_rows:
            return rows
        # MySQL reports *changed* rows: re-taking a lock with byte-identical
        # values (same clock tick, same owner) yields 0 even though the row is
        # ours. Confirm by reading it back instead of trusting the count.
        if rows == 0 and self._confirm_takeover(task_id, owner_id, expires_at):
            return 1
        return rows

    def _confirm_takeover(
        self,
        task_id: str,
        owner_id: str,
        expires_at: datetime,
    ) -> bool:
        """True when the lock row now carries exactly the values we just wrote."""
        row = self._fetch_one(Q3_CONFIRM_TAKEOVER, (task_id,))
        if row is None:
            return False
        stored_owner, stored_expires = row[0], row[1]
        if stored_owner != owner_id:
            return False
        return self._normalize_stored_time(stored_expires) == self._bind_time(expires_at)

    def _normalize_stored_time(self, value: Any) -> Any:
        """Map a driver-returned timestamp onto the dialect's bound form."""
        if isinstance(value, datetime):
            return self._bind_time(value)
        return value

    def upsert_version(
        self,
        task_id: str,
        version: str,
        status: str,
        executed_at: datetime,
        owner_id: str,
    ) -> None:
        sql = (
            Q4_UPSERT_VERSION_ON_DUPLICATE_KEY
            if self.dialect is Dialect.MYSQL
            else Q4_UPSERT_VERSION_ON_CONFLICT
        )
        self._execute(
            sql,
            (
                task_id,
                version,
                status,
                self._bind_time(executed_at),
                owner_id,
            ),
        )

    def release_lock(self, task_id: str, owner_id: str) -> None:
        self._execute(Q5_RELEASE_LOCK, (task_id, owner_id))

    def execute_ddl(self, script: str) -> None:
        for statement in split_sql_script(script):
            self._execute(statement, ())


class PostgresStore(SqlStore):
    """PostgreSQL store (primary key conflicts = SQLState ``23505``)."""

    dialect = Dialect.POSTGRES
    _paramstyle = "format"

    def __init__(self, dsn: str | None = None, *, connect: Any = None, autocommit: bool = True) -> None:
        super().__init__()
        self._dsn = dsn
        self._connect_factory = connect
        self._autocommit = autocommit
        self._conn: Any = None

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        with self._lock:
            conn = self._conn
            if conn is None or getattr(conn, "closed", 0):
                conn = self._new_connection()
                self._conn = conn
            try:
                yield conn
            except Exception:
                # autocommit keeps the connection usable; nothing to roll back.
                raise

    def _new_connection(self) -> Any:
        if self._connect_factory is not None:
            conn = self._connect_factory()
        else:
            try:
                import psycopg2
            except ImportError as exc:  # pragma: no cover - depends on env
                raise StoreError(
                    "PostgresStore requires psycopg2-binary (pip install psycopg2-binary)"
                ) from exc
            if not self._dsn:
                raise StoreError("PostgresStore requires a dsn")
            conn = psycopg2.connect(self._dsn)
        conn.autocommit = self._autocommit
        return conn

    def _bind_time(self, value: datetime) -> datetime:
        """Bind the UTC wall clock as a naive ``datetime``.

        The columns are ``TIMESTAMP`` (without time zone), so the value must be
        the UTC wall clock. psycopg2 renders a tz-aware ``datetime`` by
        converting it into the *session* time zone, so on a server whose
        ``TimeZone`` is not UTC (e.g. ``PRC``) the stored value would silently
        shift -- and two pods with different session time zones would then
        disagree about lock expiry. Naive UTC is stable regardless of session
        settings and round-trips unchanged.
        """
        return to_utc_naive(value)

    def _is_duplicate_key(self, exc: BaseException) -> bool:
        pgcode = getattr(exc, "pgcode", None)
        if pgcode == "23505":
            return True
        diag = getattr(exc, "diag", None)
        return getattr(diag, "sqlstate", None) == "23505"

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                finally:
                    self._conn = None


class MysqlStore(SqlStore):
    """MySQL / MariaDB store (vendor code ``1062``, SQLState ``23000``)."""

    dialect = Dialect.MYSQL
    _paramstyle = "format"
    #: MySQL reports changed rows, not matched rows, unless CLIENT_FOUND_ROWS
    #: is set; an external connection factory may not set it, so Q3 always
    #: confirms a zero row count by reading the row back.
    _q3_rowcount_is_matched_rows = False

    def __init__(self, dsn: str | None = None, *, connect: Any = None, **kwargs: Any) -> None:
        super().__init__()
        self._dsn = dsn
        self._connect_factory = connect
        self._kwargs = kwargs
        self._conn: Any = None

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        with self._lock:
            conn = self._conn
            if conn is None:
                conn = self._new_connection()
                self._conn = conn
            yield conn

    def _new_connection(self) -> Any:
        if self._connect_factory is not None:
            conn = self._connect_factory()
        else:
            try:
                import pymysql
                from pymysql.constants import CLIENT
            except ImportError as exc:  # pragma: no cover - depends on env
                raise StoreError(
                    "MysqlStore requires PyMySQL (pip install PyMySQL)"
                ) from exc
            if not self._dsn:
                raise StoreError("MysqlStore requires a dsn")
            kwargs = parse_mysql_dsn(self._dsn)
            kwargs.update(self._kwargs)
            kwargs.setdefault("client_flag", CLIENT.FOUND_ROWS)
            conn = pymysql.connect(**kwargs)
        # pymysql toggles autocommit through a method; other drivers expose a
        # bool property. Either way each statement stands on its own.
        set_autocommit = getattr(conn, "autocommit", None)
        if callable(set_autocommit):
            set_autocommit(True)
        elif set_autocommit is not None:
            conn.autocommit = True
        return conn

    def _bind_time(self, value: datetime) -> datetime:
        return to_utc_naive(value)

    def _is_duplicate_key(self, exc: BaseException) -> bool:
        args = getattr(exc, "args", ())
        if args and args[0] == 1062:
            return True
        if getattr(exc, "errno", None) == 1062:
            return True
        return False

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                finally:
                    self._conn = None


class SqliteStore(SqlStore):
    """SQLite store used for tests and embedded deployments.

    Every statement uses a fresh connection (``isolation_level=None``, i.e.
    autocommit, so the Q2 ``INSERT`` is immediately visible/atomic), which makes
    a single store instance safe to share between threads.
    """

    dialect = Dialect.SQLITE
    _paramstyle = "qmark"

    def __init__(
        self,
        path: str | os.PathLike[str] = ":memory:",
        *,
        busy_timeout_ms: int = 10000,
        journal_mode: str = "WAL",
        connect: Any = None,
    ) -> None:
        super().__init__()
        self.path = str(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.journal_mode = journal_mode
        self._connect_factory = connect
        self._wal_applied = False

    def _new_connection(self) -> sqlite3.Connection:
        if self._connect_factory is not None:
            conn = self._connect_factory()
        else:
            conn = sqlite3.connect(
                self.path,
                timeout=self.busy_timeout_ms / 1000.0,
                isolation_level=None,  # autocommit: every statement commits
                check_same_thread=False,
            )
        conn.execute(f"PRAGMA busy_timeout = {int(self.busy_timeout_ms)}")
        if self.journal_mode:
            self._apply_journal_mode(conn)
        return conn

    def _apply_journal_mode(self, conn: sqlite3.Connection) -> None:
        if self._wal_applied:
            return
        try:
            if self.path != ":memory:":
                conn.execute(f"PRAGMA journal_mode = {self.journal_mode}")
            self._wal_applied = True
        except sqlite3.Error:
            # A racing connection holds the write lock; the mode is already
            # persisted in the database file after the first successful call.
            pass

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        conn = self._new_connection()
        try:
            yield conn
        finally:
            conn.close()

    def _bind_time(self, value: datetime) -> str:
        return to_utc_naive(value).strftime(SQLITE_TIME_FORMAT)

    def _is_duplicate_key(self, exc: BaseException) -> bool:
        code = getattr(exc, "sqlite_errorcode", None)
        if code in (1555, 2067):
            return True
        message = str(exc).lower()
        if "unique constraint failed" in message or "primary key" in message:
            return True
        sqlstate = getattr(exc, "sqlstate", None)
        return sqlstate == "23505"

    def close(self) -> None:
        """Connections are per-statement; nothing to release."""


def parse_mysql_dsn(dsn: str) -> dict[str, Any]:
    """Split ``mysql://user:password@host:port/db?charset=utf8mb4`` into kwargs.

    PyMySQL's ``connect()`` takes keyword arguments only (no libpq-style DSN),
    so the URL is decomposed here. Query-string entries become keyword
    arguments; :meth:`MysqlStore` explicit kwargs win over them.
    """
    parts = urlsplit(dsn)
    if parts.scheme not in ("mysql", "mysql+pymysql", "mariadb", "mariadb+pymysql"):
        raise StoreError(
            f"unsupported mysql dsn scheme {parts.scheme!r}"
            " (expected mysql://user:password@host:port/database)"
        )
    kwargs: dict[str, Any] = {}
    if parts.hostname:
        kwargs["host"] = parts.hostname
    if parts.port:
        kwargs["port"] = parts.port
    if parts.username:
        kwargs["user"] = unquote(parts.username)
    if parts.password is not None:
        kwargs["password"] = unquote(parts.password)
    database = parts.path.lstrip("/")
    if database:
        # pymysql >= 2 deprecates ``db`` in favour of ``database``.
        kwargs["database"] = unquote(database)
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        kwargs[key] = value
    return kwargs


def split_sql_script(script: str) -> list[str]:
    """Split a DDL script on ``;``, dropping comments and blank statements."""
    statements: list[str] = []
    buffer: list[str] = []
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(buffer).strip().rstrip(";").strip()
            if statement:
                statements.append(statement)
            buffer = []
    tail = "\n".join(buffer).strip().rstrip(";").strip()
    if tail:
        statements.append(tail)
    return statements


def to_utc_aware(value: datetime) -> datetime:
    """Normalize to an aware UTC ``datetime`` (naive input is assumed UTC)."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def to_utc_naive(value: datetime) -> datetime:
    """Normalize to a naive ``datetime`` carrying UTC wall-clock values."""
    return to_utc_aware(value).replace(tzinfo=None)


def format_sqlite_time(value: datetime) -> str:
    """``YYYY-MM-DD HH:MM:SS.ffffff`` (UTC), lexicographic order == time order."""
    return to_utc_naive(value).strftime(SQLITE_TIME_FORMAT)


def parse_sqlite_time(value: str) -> datetime:
    return datetime.strptime(value, SQLITE_TIME_FORMAT).replace(tzinfo=timezone.utc)


__all__ = [
    "Dialect",
    "DuplicateKeyError",
    "MysqlStore",
    "PostgresStore",
    "Q1_EXISTS_SUCCESS",
    "Q2_INSERT_LOCK",
    "Q3_CONFIRM_TAKEOVER",
    "Q3_TAKEOVER_LOCK",
    "Q4_UPSERT_VERSION_ON_CONFLICT",
    "Q4_UPSERT_VERSION_ON_DUPLICATE_KEY",
    "Q5_RELEASE_LOCK",
    "SqlStore",
    "SqliteStore",
    "Store",
    "StoreError",
    "format_sqlite_time",
    "parse_mysql_dsn",
    "parse_sqlite_time",
    "split_sql_script",
    "to_utc_aware",
    "to_utc_naive",
]
