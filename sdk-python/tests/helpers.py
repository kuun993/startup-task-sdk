"""Raw-SQL helpers: inspect and arrange SQLite table contents directly."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from startup_task import SqliteStore
from startup_task.store import format_sqlite_time

BUSY_TIMEOUT_MS = 10000


def connect(db_path: Path) -> sqlite3.Connection:
    """Independent raw connection used to inspect/arrange table contents."""
    conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=10.0)
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    return conn


def new_store(db_path: Path) -> SqliteStore:
    """A fresh store == a fresh set of sqlite connections (concurrency tests)."""
    return SqliteStore(db_path, busy_timeout_ms=BUSY_TIMEOUT_MS, journal_mode="WAL")


def lock_rows(db_path: Path) -> list[tuple[str, str, str, str]]:
    conn = connect(db_path)
    try:
        return list(
            conn.execute(
                "SELECT task_id, owner_id, acquired_at, expires_at"
                " FROM task_execution_lock ORDER BY task_id"
            )
        )
    finally:
        conn.close()


def version_rows(db_path: Path) -> list[tuple[str, str, str, str, str]]:
    conn = connect(db_path)
    try:
        return list(
            conn.execute(
                "SELECT task_id, version, status, executed_at, owner_id"
                " FROM task_version_record ORDER BY task_id, version"
            )
        )
    finally:
        conn.close()


def insert_raw_lock(
    db_path: Path,
    task_id: str,
    owner_id: str,
    *,
    expires_in_seconds: float,
) -> None:
    """Insert a lock row directly, bypassing the coordinator."""
    now = datetime.now(timezone.utc)
    conn = connect(db_path)
    try:
        conn.execute(
            "INSERT INTO task_execution_lock"
            " (task_id, owner_id, acquired_at, expires_at) VALUES (?, ?, ?, ?)",
            (
                task_id,
                owner_id,
                format_sqlite_time(now - timedelta(seconds=60)),
                format_sqlite_time(now + timedelta(seconds=expires_in_seconds)),
            ),
        )
    finally:
        conn.close()


def steal_lock(db_path: Path, task_id: str, new_owner: str) -> None:
    """Simulate a takeover: reassign the lock row to another owner."""
    conn = connect(db_path)
    try:
        cursor = conn.execute(
            "UPDATE task_execution_lock SET owner_id = ? WHERE task_id = ?",
            (new_owner, task_id),
        )
        assert cursor.rowcount == 1, "expected exactly one lock row to steal"
    finally:
        conn.close()


def lock_owner(db_path: Path, task_id: str) -> str | None:
    conn = connect(db_path)
    try:
        row = conn.execute(
            "SELECT owner_id FROM task_execution_lock WHERE task_id = ?", (task_id,)
        ).fetchone()
        return None if row is None else row[0]
    finally:
        conn.close()
