"""Shared plumbing for the real-database integration tests.

DSNs come from the environment only (``STARTUP_TASK_PG_DSN`` /
``STARTUP_TASK_MYSQL_DSN``); nothing is read from or written to disk and no
credential ever reaches the assertion output. When the matching variable is
missing the tests skip, so ``pytest -q`` stays green without a database.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Iterator

import pytest

from startup_task import (
    MysqlStore,
    PostgresStore,
    TaskSpec,
    ensure_schema,
    parse_spec,
)

PG = "pg"
MYSQL = "mysql"

DSN_ENV = {PG: "STARTUP_TASK_PG_DSN", MYSQL: "STARTUP_TASK_MYSQL_DSN"}
DRIVER_NAME = {PG: "psycopg2", MYSQL: "pymysql"}

try:  # imported lazily by the stores themselves, but the tests need the modules
    import psycopg2 as _psycopg2
except ImportError:  # pragma: no cover - depends on the environment
    _psycopg2 = None

try:
    import pymysql as _pymysql
except ImportError:  # pragma: no cover - depends on the environment
    _pymysql = None

DRIVERS = {PG: _psycopg2, MYSQL: _pymysql}


def dsn_for(kind: str) -> str | None:
    return os.environ.get(DSN_ENV[kind]) or None


def skip_reason(kind: str) -> str:
    return f"{DSN_ENV[kind]} is not set"


def driver_available(kind: str) -> bool:
    return DRIVERS[kind] is not None


def missing_dsn_marker(kind: str):
    return pytest.mark.skipif(dsn_for(kind) is None, reason=skip_reason(kind))


def build_store(kind: str, dsn: str):
    if kind == PG:
        return PostgresStore(dsn)
    if kind == MYSQL:
        return MysqlStore(dsn)
    raise ValueError(f"unknown database kind {kind!r}")


def custom_spec(
    task_id: str,
    version: str = "v1",
    handler: str = "test_handler",
    timeout_seconds: int = 300,
) -> TaskSpec:
    """A ``custom`` task spec built through the real YAML loader."""
    return parse_spec(
        f"task_id: {task_id}\n"
        f'version: "{version}"\n'
        f"timeout_seconds: {timeout_seconds}\n"
        "execution:\n"
        "  executor: custom\n"
        f"  handler: {handler}\n"
    )


class FixedClock:
    """Deterministic clock so bound timestamps can be asserted exactly."""

    def __init__(self, now: datetime) -> None:
        self._now = now
        self._monotonic = 1000.0

    def now_utc(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._monotonic


class DbHarness:
    """Owns the store instances and the rows a single test creates."""

    def __init__(self, kind: str, dsn: str) -> None:
        self.kind = kind
        self.dsn = dsn
        self._stores: list[Any] = []
        self._task_ids: list[str] = []
        self._reader: Any = None

    # ------------------------------------------------------------- lifecycle

    def register(self, store: Any) -> Any:
        """Track an externally built store so it is closed at teardown."""
        self._stores.append(store)
        return store

    def store(self) -> Any:
        """A new store == a new independent connection."""
        return self.register(build_store(self.kind, self.dsn))

    def reader(self) -> Any:
        """Dedicated store used to inspect rows from another connection."""
        if self._reader is None:
            self._reader = self.store()
        return self._reader

    def task_id(self, case: str) -> str:
        task_id = f"it/{self.kind}-{case}-{uuid.uuid4().hex[:12]}"
        self._task_ids.append(task_id)
        return task_id

    def close(self) -> None:
        if self._task_ids:
            cleaner = None
            try:
                cleaner = build_store(self.kind, self.dsn)
                for task_id in self._task_ids:
                    self._delete_rows(cleaner, task_id)
            except Exception:  # noqa: BLE001 - teardown must not mask failures
                pass
            finally:
                if cleaner is not None:
                    _quiet_close(cleaner)
        for store in self._stores:
            _quiet_close(store)

    def _delete_rows(self, store: Any, task_id: str) -> None:
        store._execute(
            "DELETE FROM task_execution_lock WHERE task_id = ?", (task_id,)
        )
        store._execute(
            "DELETE FROM task_version_record WHERE task_id = ?", (task_id,)
        )

    # ------------------------------------------------------------ inspection

    def lock_row(self, task_id: str) -> tuple[Any, Any, Any] | None:
        return self.reader()._fetch_one(
            "SELECT owner_id, acquired_at, expires_at"
            " FROM task_execution_lock WHERE task_id = ?",
            (task_id,),
        )

    def lock_count(self, task_id: str) -> int:
        return self._count("task_execution_lock", task_id)

    def version_count(self, task_id: str) -> int:
        return self._count("task_version_record", task_id)

    def version_row(self, task_id: str, version: str = "v1"):
        return self.reader()._fetch_one(
            "SELECT status, executed_at, owner_id FROM task_version_record"
            " WHERE task_id = ? AND version = ?",
            (task_id, version),
        )

    def _count(self, table: str, task_id: str) -> int:
        row = self.reader()._fetch_one(
            f"SELECT COUNT(*) FROM {table} WHERE task_id = ?", (task_id,)
        )
        return int(row[0])

    def steal_lock(self, task_id: str, new_owner: str) -> int:
        """Simulate a takeover by renaming the lock owner."""
        return self.reader()._execute(
            "UPDATE task_execution_lock SET owner_id = ? WHERE task_id = ?",
            (new_owner, task_id),
        )

    def column(self, table: str, column: str) -> dict[str, Any]:
        """Column metadata from ``information_schema`` (dialect aware)."""
        if self.kind == PG:
            row = self.reader()._fetch_one(
                "SELECT data_type, character_maximum_length"
                " FROM information_schema.columns"
                " WHERE table_schema = current_schema()"
                "   AND table_name = ? AND column_name = ?",
                (table, column),
            )
            if row is None:
                raise AssertionError(f"column {table}.{column} does not exist")
            return {"data_type": row[0], "char_length": row[1]}

        row = self.reader()._fetch_one(
            "SELECT data_type, character_maximum_length, datetime_precision"
            " FROM information_schema.columns"
            " WHERE table_schema = database()"
            "   AND table_name = ? AND column_name = ?",
            (table, column),
        )
        if row is None:
            raise AssertionError(f"column {table}.{column} does not exist")
        return {
            "data_type": row[0],
            "char_length": row[1],
            "datetime_precision": None if row[2] is None else int(row[2]),
        }


def _quiet_close(store: Any) -> None:
    try:
        store.close()
    except Exception:  # noqa: BLE001 - teardown only
        pass


def open_harness(kind: str) -> DbHarness:
    """Create a harness with the schema in place; skips when unusable."""
    if not dsn_for(kind):
        pytest.skip(skip_reason(kind))
    if not driver_available(kind):
        pytest.skip(f"{DRIVER_NAME[kind]} is not installed")
    harness = DbHarness(kind, dsn_for(kind) or "")
    try:
        ensure_schema(harness.store())
    except Exception:
        harness.close()
        raise
    return harness


@contextlib.contextmanager
def harness_context(kind: str) -> Iterator[DbHarness]:
    harness = open_harness(kind)
    try:
        yield harness
    finally:
        harness.close()


def utc(*parts: int) -> datetime:
    return datetime(*parts, tzinfo=timezone.utc)


__all__ = [
    "DRIVERS",
    "DSN_ENV",
    "DbHarness",
    "FixedClock",
    "MYSQL",
    "PG",
    "build_store",
    "custom_spec",
    "driver_available",
    "dsn_for",
    "harness_context",
    "missing_dsn_marker",
    "open_harness",
    "skip_reason",
    "utc",
]
