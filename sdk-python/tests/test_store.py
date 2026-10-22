"""Store / dialect behaviour: Q2 conflict detection, Q3, Q4, Q5, time binding."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from helpers import connect, lock_owner, lock_rows, new_store, version_rows
from startup_task import (
    Dialect,
    DuplicateKeyError,
    MysqlStore,
    PostgresStore,
    SqliteStore,
    Store,
    StoreError,
    ensure_schema,
    format_sqlite_time,
    parse_sqlite_time,
)
from startup_task.schema import ddl_for, schema_statements
from startup_task.store import parse_mysql_dsn, split_sql_script, to_utc_naive

TASK_ID = "store/task"
NOW = datetime(2024, 6, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)
EXPIRES = NOW + timedelta(seconds=60)


def test_sqlite_store_satisfies_store_protocol(store):
    assert isinstance(store, Store)
    assert store.dialect is Dialect.SQLITE


def test_ensure_schema_is_idempotent(db_path):
    store = new_store(db_path)
    ensure_schema(store)
    ensure_schema(store)
    conn = connect(db_path)
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    finally:
        conn.close()
    assert {"task_execution_lock", "task_version_record"} <= tables


def test_sqlite_binds_time_as_utc_text_with_microseconds(store, db_path):
    local = NOW.astimezone(timezone(timedelta(hours=8)))
    store.insert_lock(TASK_ID, "pod-x", local, local + timedelta(seconds=30))

    row = lock_rows(db_path)[0]
    assert row[0] == TASK_ID
    assert row[1] == "pod-x"
    assert row[2] == "2024-06-01 12:00:00.123456"
    assert row[3] == "2024-06-01 12:00:30.123456"
    assert format_sqlite_time(local) == "2024-06-01 12:00:00.123456"
    assert parse_sqlite_time(row[2]) == NOW


def test_duplicate_insert_lock_raises_duplicate_key(store):
    store.insert_lock(TASK_ID, "pod-a", NOW, NOW + timedelta(seconds=60))

    with pytest.raises(DuplicateKeyError):
        store.insert_lock(TASK_ID, "pod-b", NOW, NOW + timedelta(seconds=60))

    assert lock_owner_for(store, TASK_ID) == "pod-a"


def lock_owner_for(store, task_id: str) -> str | None:
    row = store._fetch_one(
        "SELECT owner_id FROM task_execution_lock WHERE task_id = ?", (task_id,)
    )
    return None if row is None else row[0]


def test_takeover_only_when_expired(store):
    store.insert_lock(TASK_ID, "pod-old", NOW, NOW + timedelta(seconds=60))

    assert store.takeover_lock("pod-new", NOW, NOW + timedelta(seconds=60), TASK_ID, NOW) == 0
    assert lock_owner_for(store, TASK_ID) == "pod-old"

    later = NOW + timedelta(seconds=61)
    assert (
        store.takeover_lock(
            "pod-new", later, later + timedelta(seconds=60), TASK_ID, later
        )
        == 1
    )
    assert lock_owner_for(store, TASK_ID) == "pod-new"


def test_upsert_version_updates_existing_row(store, db_path):
    store.upsert_version(TASK_ID, "v1", "failed", NOW, "pod-a")
    store.upsert_version(TASK_ID, "v1", "success", NOW, "pod-b")

    rows = version_rows(db_path)
    assert len(rows) == 1
    assert rows[0][2] == "success"
    assert rows[0][4] == "pod-b"


def test_release_lock_is_owner_scoped(store, db_path):
    store.insert_lock(TASK_ID, "pod-a", NOW, NOW + timedelta(seconds=60))

    store.release_lock(TASK_ID, "pod-b")
    assert lock_owner_for(store, TASK_ID) == "pod-a"

    store.release_lock(TASK_ID, "pod-a")
    assert lock_rows(db_path) == []


def test_exists_success_only_accepts_success(store):
    store.upsert_version(TASK_ID, "v1", "failed", NOW, "pod-a")
    assert store.exists_success(TASK_ID, "v1") is False

    store.upsert_version(TASK_ID, "v1", "success", NOW, "pod-a")
    assert store.exists_success(TASK_ID, "v1") is True
    assert store.exists_success(TASK_ID, "v2") is False


def test_sqlite_duplicate_key_classification():
    store = SqliteStore(":memory:")
    unique = sqlite3.IntegrityError("UNIQUE constraint failed: x.y")
    primary = sqlite3.IntegrityError("PRIMARY KEY must be unique")
    other = sqlite3.IntegrityError("NOT NULL constraint failed: x.y")

    assert store._is_duplicate_key(unique) is True
    assert store._is_duplicate_key(primary) is True
    assert store._is_duplicate_key(other) is False


def test_postgres_duplicate_key_classification():
    store = PostgresStore("postgresql:///unused")

    class UniqueViolation(Exception):
        pgcode = "23505"

    class Other(Exception):
        pgcode = "42P01"

    assert store._is_duplicate_key(UniqueViolation()) is True
    assert store._is_duplicate_key(Other()) is False
    assert store._is_duplicate_key(Exception("boom")) is False
    store.close()


def test_mysql_duplicate_key_classification():
    store = MysqlStore("mysql:///unused")

    class IntegrityError(Exception):
        pass

    error = IntegrityError(1062, "Duplicate entry 'x' for key 'PRIMARY'")
    assert store._is_duplicate_key(error) is True
    assert store._is_duplicate_key(IntegrityError(1452, "fk")) is False
    store.close()


def test_parse_mysql_dsn_maps_url_parts():
    kwargs = parse_mysql_dsn(
        "mysql://u%40x:p%3Aw@db.example:33306/app?charset=utf8mb4&connect_timeout=5"
    )

    assert kwargs == {
        "host": "db.example",
        "port": 33306,
        "user": "u@x",
        "password": "p:w",
        "database": "app",
        "charset": "utf8mb4",
        "connect_timeout": "5",
    }


def test_parse_mysql_dsn_rejects_foreign_scheme():
    with pytest.raises(StoreError) as excinfo:
        parse_mysql_dsn("postgresql://user:pw@host:5432/db")
    assert "mysql" in str(excinfo.value)


def test_postgres_and_mysql_use_format_paramstyle():
    postgres = PostgresStore("postgresql:///unused")
    mysql = MysqlStore("mysql:///unused")
    try:
        assert postgres._sql("SELECT 1 WHERE a = ? AND b = ?") == (
            "SELECT 1 WHERE a = %s AND b = %s"
        )
        assert mysql._sql("SELECT 1 WHERE a = ?") == "SELECT 1 WHERE a = %s"
        assert SqliteStore(":memory:")._sql("SELECT 1 WHERE a = ?") == (
            "SELECT 1 WHERE a = ?"
        )
    finally:
        postgres.close()
        mysql.close()


def test_time_binding_per_dialect():
    naive = datetime(2024, 6, 1, 12, 0, 0)
    aware = datetime(2024, 6, 1, 20, 0, 0, tzinfo=timezone(timedelta(hours=8)))

    postgres = PostgresStore("postgresql:///unused")
    mysql = MysqlStore("mysql:///unused")
    sqlite = SqliteStore(":memory:")
    try:
        # TIMESTAMP (no zone) carries the UTC wall clock. Binding an aware
        # datetime would make psycopg2 render it in the session time zone.
        bound = postgres._bind_time(aware)
        assert isinstance(bound, datetime)
        assert bound.tzinfo is None
        assert bound == datetime(2024, 6, 1, 12, 0, 0)

        bound = mysql._bind_time(aware)
        assert isinstance(bound, datetime)
        assert bound.tzinfo is None
        assert bound == datetime(2024, 6, 1, 12, 0, 0)

        assert sqlite._bind_time(naive) == "2024-06-01 12:00:00.000000"
        assert sqlite._bind_time(aware) == "2024-06-01 12:00:00.000000"
    finally:
        postgres.close()
        mysql.close()


def test_store_error_is_not_duplicate_key(store):
    with pytest.raises(StoreError) as excinfo:
        store._execute("SELECT * FROM table_that_does_not_exist", ())
    assert not isinstance(excinfo.value, DuplicateKeyError)
    assert "no such table" in str(excinfo.value)


def test_ddl_scripts_match_spec_files():
    ddl_files = {
        Dialect.POSTGRES: "postgres.sql",
        Dialect.MYSQL: "mysql.sql",
        Dialect.SQLITE: "sqlite.sql",
    }
    spec_dir = None
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "spec" / "ddl"
        if candidate.is_dir():
            spec_dir = candidate
            break
    if spec_dir is None:  # installed package without the repository
        pytest.skip("spec/ddl not available")

    for dialect, filename in ddl_files.items():
        expected = split_sql_script((spec_dir / filename).read_text(encoding="utf-8"))
        assert schema_statements(dialect) == expected


def test_split_sql_script_strips_comments():
    script = """
-- comment
CREATE TABLE a (x TEXT);

CREATE TABLE b (x TEXT);
"""
    assert split_sql_script(script) == [
        "CREATE TABLE a (x TEXT)",
        "CREATE TABLE b (x TEXT)",
    ]


def test_schema_statements_cover_both_tables():
    for dialect in Dialect:
        statements = schema_statements(dialect)
        assert len(statements) == 2
        assert any("task_execution_lock" in s for s in statements)
        assert any("task_version_record" in s for s in statements)


# ---------------------------------------------------------------------------
# MySQL Q3: rowcount counts *changed* rows, so an identical re-takeover reports
# 0. The row is then read back to decide whether we hold the lock.
# ---------------------------------------------------------------------------


def _mysql_takeover_probe(monkeypatch, *, rowcount: int, row, expires_at=EXPIRES):
    store = MysqlStore("mysql:///unused")
    selects: list[tuple[str, tuple]] = []

    def fake_execute(sql, params=(), *, classify_duplicate=False):
        return rowcount

    def fake_fetch(sql, params=()):
        selects.append((sql, tuple(params)))
        return row

    monkeypatch.setattr(store, "_execute", fake_execute)
    monkeypatch.setattr(store, "_fetch_one", fake_fetch)
    rows = store.takeover_lock("pod-new", NOW, expires_at, TASK_ID, NOW)
    return rows, selects


def test_mysql_takeover_confirms_zero_rowcount(monkeypatch):
    rows, selects = _mysql_takeover_probe(
        monkeypatch, rowcount=0, row=("pod-new", to_utc_naive(EXPIRES))
    )

    assert rows == 1, "rowcount=0 with a matching row must count as acquired"
    assert len(selects) == 1
    sql, params = selects[0]
    assert "task_execution_lock" in sql
    assert params == (TASK_ID,)


def test_mysql_takeover_rejects_zero_rowcount_when_owner_differs(monkeypatch):
    rows, selects = _mysql_takeover_probe(
        monkeypatch, rowcount=0, row=("pod-old", to_utc_naive(EXPIRES))
    )

    assert rows == 0
    assert len(selects) == 1


def test_mysql_takeover_rejects_zero_rowcount_when_expiry_differs(monkeypatch):
    rows, _ = _mysql_takeover_probe(
        monkeypatch,
        rowcount=0,
        row=("pod-new", to_utc_naive(EXPIRES + timedelta(seconds=1))),
    )

    assert rows == 0


def test_mysql_takeover_rejects_zero_rowcount_when_row_is_gone(monkeypatch):
    rows, _ = _mysql_takeover_probe(monkeypatch, rowcount=0, row=None)

    assert rows == 0


def test_mysql_takeover_skips_confirmation_on_rowcount_one(monkeypatch):
    rows, selects = _mysql_takeover_probe(monkeypatch, rowcount=1, row=None)

    assert rows == 1
    assert selects == []


def test_postgres_uses_rowcount_without_confirmation(monkeypatch):
    store = PostgresStore("postgresql:///unused")
    monkeypatch.setattr(store, "_execute", lambda *a, **kw: 0)

    def unexpected(*args, **kwargs):
        raise AssertionError("PG reports matched rows, no read-back expected")

    monkeypatch.setattr(store, "_fetch_one", unexpected)
    assert store.takeover_lock("pod-new", NOW, EXPIRES, TASK_ID, NOW) == 0
    store.close()


def test_sqlite_uses_rowcount_without_confirmation(store, monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("SQLite reports matched rows, no read-back expected")

    monkeypatch.setattr(store, "_fetch_one", unexpected)
    store.insert_lock(TASK_ID, "pod-old", NOW, NOW - timedelta(seconds=1))
    assert store.takeover_lock("pod-new", NOW, EXPIRES, TASK_ID, NOW) == 1


# ---------------------------------------------------------------------------
# Transaction state: statements are autocommitted one by one, and a failed
# statement must not leave an aborted/unfinished transaction behind.
# ---------------------------------------------------------------------------


class _FakeUniqueViolation(Exception):
    pgcode = "23505"


class _FakeCursor:
    def __init__(self, conn) -> None:
        self._conn = conn
        self.rowcount = -1
        self._rows: list[tuple] = []

    def execute(self, sql, params=None):
        self._conn.statements.append(sql)
        if self._conn.fail_on and self._conn.fail_on in sql:
            raise self._conn.fail_exception
        if sql.startswith("UPDATE"):
            self.rowcount = 1
        elif sql.startswith("SELECT"):
            self._rows = list(self._conn.select_rows)
            self.rowcount = len(self._rows)
        else:
            self.rowcount = 1
        return self

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self) -> None:
        pass


class _FakeConnection:
    """Minimal DB-API connection recording commit/rollback calls."""

    def __init__(self, fail_on=None, fail_exception=None, select_rows=()) -> None:
        self.autocommit = True  # overwritten by PostgresStore(autocommit=...)
        self.events: list[str] = []
        self.statements: list[str] = []
        self.fail_on = fail_on
        self.fail_exception = fail_exception
        self.select_rows = list(select_rows)

    def cursor(self):
        return _FakeCursor(self)

    def commit(self) -> None:
        self.events.append("commit")

    def rollback(self) -> None:
        self.events.append("rollback")

    def close(self) -> None:
        self.events.append("close")


def test_duplicate_key_rolls_back_before_takeover():
    fake = _FakeConnection(
        fail_on="INSERT INTO task_execution_lock",
        fail_exception=_FakeUniqueViolation("duplicate key value"),
    )
    store = PostgresStore(connect=lambda: fake, autocommit=False)

    with pytest.raises(DuplicateKeyError):
        store.insert_lock(TASK_ID, "pod-a", NOW, EXPIRES)

    # The aborted transaction is cleared before Q3 runs, so PG cannot answer 25P02.
    assert fake.events == ["rollback"]

    assert store.takeover_lock("pod-b", NOW, EXPIRES, TASK_ID, NOW) == 1
    assert fake.events == ["rollback", "commit"]
    assert any("UPDATE task_execution_lock" in s for s in fake.statements)
    store.close()


def test_non_autocommit_connection_commits_each_statement():
    fake = _FakeConnection()
    store = PostgresStore(connect=lambda: fake, autocommit=False)

    store.insert_lock(TASK_ID, "pod-a", NOW, EXPIRES)
    store.upsert_version(TASK_ID, "v1", "success", NOW, "pod-a")
    store.release_lock(TASK_ID, "pod-a")

    assert fake.events == ["commit", "commit", "commit"]
    store.close()


def test_failed_release_rolls_back_open_transaction():
    fake = _FakeConnection(
        fail_on="DELETE FROM task_execution_lock",
        fail_exception=RuntimeError("delete denied"),
    )
    store = PostgresStore(connect=lambda: fake, autocommit=False)

    with pytest.raises(StoreError):
        store.release_lock(TASK_ID, "pod-a")

    assert fake.events == ["rollback"]
    store.close()


def test_autocommit_connection_is_not_committed_explicitly():
    fake = _FakeConnection()
    store = PostgresStore(connect=lambda: fake, autocommit=True)

    store.insert_lock(TASK_ID, "pod-a", NOW, EXPIRES)

    assert fake.events == []
    store.close()
