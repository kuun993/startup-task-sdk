"""PostgreSQL-only integration checks: transaction state and time binding.

Both areas are invisible on SQLite: psycopg2 enters the aborted state after a
failed statement unless the transaction is cleared, and it renders an aware
``datetime`` in the *session* time zone, so the server's ``TimeZone`` setting
(``PRC`` here) would otherwise shift every stored timestamp.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from integration_support import (
    FixedClock,
    PG,
    custom_spec,
    missing_dsn_marker,
    utc,
)
from startup_task import Coordinator, Outcome, PostgresStore

psycopg2 = pytest.importorskip("psycopg2")

pytestmark = missing_dsn_marker(PG)

NON_UTC_SESSION_TZ = "America/New_York"


def _store_with_session(dsn: str, *, autocommit: bool, time_zone: str | None = None):
    """A store whose connection is fully controlled by the test."""

    def factory():
        conn = psycopg2.connect(dsn)
        conn.autocommit = autocommit
        if time_zone is not None:
            with conn.cursor() as cursor:
                # The literal is a test constant, never user input.
                cursor.execute(f"SET TIME ZONE '{time_zone}'")
        return conn

    return PostgresStore(connect=factory, autocommit=autocommit)


# ---------------------------------------------------------------------------
# Non-autocommit connections: a Q2 primary-key conflict must not poison the
# connection (SQLState 25P02), so the Q3 takeover right after it still works.
# ---------------------------------------------------------------------------


def test_non_autocommit_unexpired_lock_is_skipped_locked(pg_harness):
    store = pg_harness.store()
    task_id = pg_harness.task_id("pg-nonautocommit-held")
    now = datetime.now(timezone.utc)

    store.insert_lock(task_id, "pod-holder", now, now + timedelta(seconds=300))

    contender = pg_harness.register(
        _store_with_session(pg_harness.dsn, autocommit=False)
    )

    calls: list[dict] = []
    coordinator = Coordinator(contender, owner_id="pod-contender")
    coordinator.register_handler("test_handler", calls.append)

    result = coordinator.run(custom_spec(task_id))

    # A 25P02 would surface here as FAILED; the takeover path must be clean.
    assert result.outcome is Outcome.SKIPPED_LOCKED
    assert result.error is None
    assert "25P02" not in (result.error or "")
    assert calls == []

    row = pg_harness.lock_row(task_id)
    assert row is not None and row[0] == "pod-holder"
    assert pg_harness.version_count(task_id) == 0


def test_non_autocommit_expired_lock_is_taken_over(pg_harness):
    store = pg_harness.store()
    task_id = pg_harness.task_id("pg-nonautocommit-expired")
    now = utc(2024, 6, 1, 12, 0, 0)

    store.insert_lock(
        task_id, "pod-old", now - timedelta(seconds=120), now - timedelta(seconds=30)
    )

    contender = pg_harness.register(
        _store_with_session(pg_harness.dsn, autocommit=False)
    )

    observed: dict[str, str | None] = {}

    def handler(config: dict) -> None:
        row = pg_harness.lock_row(task_id)
        observed["owner"] = None if row is None else row[0]

    coordinator = Coordinator(contender, owner_id="pod-new")
    coordinator.register_handler("test_handler", handler)

    result = coordinator.run(custom_spec(task_id))

    assert result.outcome is Outcome.EXECUTED, result.error
    assert result.error is None
    assert observed["owner"] == "pod-new"
    assert pg_harness.lock_count(task_id) == 0
    assert pg_harness.version_row(task_id)[0] == "success"


def test_non_autocommit_release_only_removes_own_lock(pg_harness):
    store = pg_harness.store()
    task_id = pg_harness.task_id("pg-nonautocommit-release")
    now = utc(2024, 6, 1, 12, 0, 0)

    store.insert_lock(task_id, "pod-a", now, now + timedelta(seconds=300))
    assert pg_harness.steal_lock(task_id, "pod-b") == 1

    contender = pg_harness.register(
        _store_with_session(pg_harness.dsn, autocommit=False)
    )
    Coordinator(contender, owner_id="pod-a")._release_lock(task_id)

    row = pg_harness.lock_row(task_id)
    assert row is not None and row[0] == "pod-b"


# ---------------------------------------------------------------------------
# Time binding: stored timestamps must be the UTC wall clock, independent of
# the PostgreSQL session time zone.
# ---------------------------------------------------------------------------


def _run_and_capture_lock_times(harness, store, task_id: str, fixed_now: datetime):
    observed: dict[str, datetime] = {}

    def handler(config: dict) -> None:
        row = harness.lock_row(task_id)
        assert row is not None
        observed["acquired_at"] = row[1]
        observed["expires_at"] = row[2]

    coordinator = Coordinator(store, owner_id="pod-a", clock=FixedClock(fixed_now))
    coordinator.register_handler("test_handler", handler)
    result = coordinator.run(custom_spec(task_id, timeout_seconds=42))
    return result, observed


def test_time_binding_stores_utc_wall_clock(pg_harness):
    store = pg_harness.store()
    task_id = pg_harness.task_id("pg-time")
    fixed_now = datetime(2024, 6, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)

    result, observed = _run_and_capture_lock_times(
        pg_harness, store, task_id, fixed_now
    )

    assert result.outcome is Outcome.EXECUTED
    expected_acquired = fixed_now.replace(tzinfo=None)
    expected_expires = (fixed_now + timedelta(seconds=42)).replace(tzinfo=None)

    assert observed["acquired_at"] == expected_acquired
    assert observed["expires_at"] == expected_expires
    assert observed["acquired_at"].microsecond == 123456
    # Reading the naive value back as UTC must land on the exact instant that
    # was bound -- this is what fails when the value is shifted by the session tz.
    assert observed["acquired_at"].replace(tzinfo=timezone.utc) == fixed_now

    status, executed_at, _owner = pg_harness.version_row(task_id)
    assert status == "success"
    assert executed_at is not None
    assert executed_at.replace(tzinfo=timezone.utc) >= fixed_now


def test_time_binding_ignores_non_utc_session_timezone(pg_harness):
    store = pg_harness.register(
        _store_with_session(
            pg_harness.dsn, autocommit=True, time_zone=NON_UTC_SESSION_TZ
        )
    )

    task_id = pg_harness.task_id("pg-time-tz")
    fixed_now = datetime(2024, 6, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)

    session_tz = store._fetch_one("SHOW TIME ZONE")[0]
    assert session_tz == NON_UTC_SESSION_TZ

    result, observed = _run_and_capture_lock_times(
        pg_harness, store, task_id, fixed_now
    )

    assert result.outcome is Outcome.EXECUTED, result.error
    assert observed["acquired_at"] == fixed_now.replace(tzinfo=None)
    assert observed["expires_at"] == (
        fixed_now + timedelta(seconds=42)
    ).replace(tzinfo=None)


def test_takeover_comparison_is_timezone_independent(pg_harness):
    """A lock written under one session tz must expire for a pod using another."""
    writer_store = pg_harness.register(
        _store_with_session(
            pg_harness.dsn, autocommit=True, time_zone=NON_UTC_SESSION_TZ
        )
    )
    task_id = pg_harness.task_id("pg-time-compare")
    now = utc(2024, 6, 1, 12, 0, 0)

    writer_store.insert_lock(
        task_id, "pod-old", now - timedelta(seconds=120), now - timedelta(seconds=30)
    )

    reader_store = pg_harness.store()
    calls: list[dict] = []
    coordinator = Coordinator(reader_store, owner_id="pod-new")
    coordinator.register_handler("test_handler", calls.append)

    result = coordinator.run(custom_spec(task_id))

    assert result.outcome is Outcome.EXECUTED, result.error
    assert calls == [{}]
    assert pg_harness.version_row(task_id)[0] == "success"
