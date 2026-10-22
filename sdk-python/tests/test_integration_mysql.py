"""MySQL / MariaDB-only integration checks.

Covers the two dialect-specific SQL paths that SQLite cannot prove: the
``ON DUPLICATE KEY UPDATE`` upsert and vendor error ``1062`` conflict
detection, plus the ``DATETIME(6)`` UTC round-trip.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from integration_support import MYSQL, FixedClock, custom_spec, missing_dsn_marker, utc
from startup_task import Coordinator, DuplicateKeyError, Outcome

pytest.importorskip("pymysql")

pytestmark = missing_dsn_marker(MYSQL)


def test_datetime6_columns_are_real_datetime(mysql_harness):
    acquired_at = mysql_harness.column("task_execution_lock", "acquired_at")
    expires_at = mysql_harness.column("task_execution_lock", "expires_at")
    executed_at = mysql_harness.column("task_version_record", "executed_at")

    for meta in (acquired_at, expires_at, executed_at):
        assert meta["data_type"] == "datetime"
        assert meta["datetime_precision"] == 6


def test_upsert_overwrites_failed_with_success(mysql_harness):
    store = mysql_harness.store()
    task_id = mysql_harness.task_id("mysql-upsert")
    first = utc(2024, 6, 1, 12, 0, 0)
    second = first + timedelta(seconds=5)

    store.upsert_version(task_id, "v1", "failed", first, "pod-a")
    store.upsert_version(task_id, "v1", "success", second, "pod-b")

    # ON DUPLICATE KEY UPDATE must leave exactly one row, not raise 1062.
    assert mysql_harness.version_count(task_id) == 1
    status, executed_at, owner_id = mysql_harness.version_row(task_id, "v1")
    assert status == "success"
    assert owner_id == "pod-b"
    assert executed_at == second.replace(tzinfo=None)


def test_duplicate_lock_raises_conflict_not_failure(mysql_harness):
    store = mysql_harness.store()
    task_id = mysql_harness.task_id("mysql-1062")
    now = datetime.now(timezone.utc)

    store.insert_lock(task_id, "pod-a", now, now + timedelta(seconds=300))

    with pytest.raises(DuplicateKeyError) as excinfo:
        store.insert_lock(task_id, "pod-b", now, now + timedelta(seconds=300))

    # The dialect layer must classify the real vendor error 1062 as a conflict.
    cause = excinfo.value.__cause__
    assert cause is not None
    assert getattr(cause, "args", (None,))[0] == 1062
    assert store._is_duplicate_key(cause) is True

    calls: list[dict] = []
    coordinator = Coordinator(store, owner_id="pod-b")
    coordinator.register_handler("test_handler", calls.append)

    result = coordinator.run(custom_spec(task_id))

    assert result.outcome is Outcome.SKIPPED_LOCKED
    assert result.error is None
    assert calls == []
    row = mysql_harness.lock_row(task_id)
    assert row is not None and row[0] == "pod-a"


def test_time_roundtrip_is_utc_datetime6(mysql_harness):
    store = mysql_harness.store()
    task_id = mysql_harness.task_id("mysql-time")
    fixed_now = datetime(2024, 6, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)
    observed: dict[str, datetime] = {}

    def handler(config: dict) -> None:
        row = mysql_harness.lock_row(task_id)
        assert row is not None
        observed["acquired_at"] = row[1]
        observed["expires_at"] = row[2]

    coordinator = Coordinator(store, owner_id="pod-a", clock=FixedClock(fixed_now))
    coordinator.register_handler("test_handler", handler)

    result = coordinator.run(custom_spec(task_id, timeout_seconds=42))

    assert result.outcome is Outcome.EXECUTED
    acquired = observed["acquired_at"]
    assert isinstance(acquired, datetime)
    assert acquired.tzinfo is None  # DATETIME(6) carries no zone ...
    assert acquired == fixed_now.replace(tzinfo=None)  # ... but UTC wall clock
    assert acquired.microsecond == 123456
    assert observed["expires_at"] == (fixed_now + timedelta(seconds=42)).replace(
        tzinfo=None
    )

    status, executed_at, _owner = mysql_harness.version_row(task_id)
    assert status == "success"
    assert isinstance(executed_at, datetime)
    assert executed_at == fixed_now.replace(tzinfo=None)


def test_datetime6_keeps_microseconds_of_the_bound_time(mysql_harness):
    """Second-precision truncation would break lock ordering under contention."""
    store = mysql_harness.store()
    task_id = mysql_harness.task_id("mysql-precision")
    now = utc(2024, 6, 1, 12, 0, 0, 987654)

    store.insert_lock(task_id, "pod-a", now, now + timedelta(seconds=300))

    row = mysql_harness.lock_row(task_id)
    assert row is not None
    assert row[1].microsecond == 987654
    assert row[2].microsecond == 987654
