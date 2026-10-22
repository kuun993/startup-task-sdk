"""Real-database scenarios, run against PostgreSQL and MySQL/MariaDB.

Everything here mirrors design.md §10 on a real server: real DDL, real
primary-key conflicts, real timestamp columns. The parameterized ``db_harness``
fixture skips a dialect whose DSN environment variable is unset.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone

from integration_support import custom_spec, utc
from startup_task import Coordinator, Outcome


def _worker_count() -> int:
    return 8


def test_ensure_schema_creates_expected_columns(db_harness):
    lock_task_id = db_harness.column("task_execution_lock", "task_id")
    assert lock_task_id["char_length"] == 128

    owner_id = db_harness.column("task_execution_lock", "owner_id")
    assert owner_id["char_length"] == 128

    acquired_at = db_harness.column("task_execution_lock", "acquired_at")
    expires_at = db_harness.column("task_execution_lock", "expires_at")
    status = db_harness.column("task_version_record", "status")
    assert status["char_length"] == 16

    if db_harness.kind == "pg":
        assert acquired_at["data_type"] == "timestamp without time zone"
        assert expires_at["data_type"] == "timestamp without time zone"
    else:
        assert acquired_at["data_type"] == "datetime"
        assert expires_at["data_type"] == "datetime"
        assert acquired_at["datetime_precision"] == 6
        assert expires_at["datetime_precision"] == 6


def test_first_run_executed(db_harness):
    store = db_harness.store()
    task_id = db_harness.task_id("first")
    calls: list[dict] = []

    coordinator = Coordinator(store, owner_id="pod-a")
    coordinator.register_handler("test_handler", calls.append)
    spec = custom_spec(task_id)

    assert db_harness.lock_count(task_id) == 0

    result = coordinator.run(spec)

    assert result.outcome is Outcome.EXECUTED
    assert result.error is None
    assert result.task_id == task_id
    assert result.version == "v1"
    assert result.duration_ms >= 0
    assert calls == [{}]

    assert db_harness.lock_count(task_id) == 0
    assert db_harness.version_count(task_id) == 1
    status, executed_at, owner_id = db_harness.version_row(task_id)
    assert status == "success"
    assert owner_id == "pod-a"
    assert executed_at is not None


def test_second_run_skipped_already_done(db_harness):
    store = db_harness.store()
    task_id = db_harness.task_id("second")
    calls: list[dict] = []

    coordinator = Coordinator(store, owner_id="pod-a")
    coordinator.register_handler("test_handler", calls.append)
    spec = custom_spec(task_id)

    assert coordinator.run(spec).outcome is Outcome.EXECUTED
    assert db_harness.lock_count(task_id) == 0

    result = coordinator.run(spec)

    assert result.outcome is Outcome.SKIPPED_ALREADY_DONE
    assert result.error is None
    assert calls == [{}]
    # The lock table is never written for an already-done version.
    assert db_harness.lock_count(task_id) == 0
    assert db_harness.version_count(task_id) == 1

    other_pod = Coordinator(db_harness.store(), owner_id="pod-b")
    other_pod.register_handler("test_handler", calls.append)
    assert other_pod.run(spec).outcome is Outcome.SKIPPED_ALREADY_DONE
    assert db_harness.lock_count(task_id) == 0


def test_concurrent_only_one_executes(db_harness):
    task_id = db_harness.task_id("concurrent")
    calls: list[dict] = []
    calls_lock = threading.Lock()
    barrier = threading.Barrier(_worker_count())

    def handler(config: dict) -> None:
        with calls_lock:
            calls.append(config)
        time.sleep(1.0)  # hold the lock while the others contend

    stores = [db_harness.store() for _ in range(_worker_count())]
    spec = custom_spec(task_id)

    results: list = [None] * _worker_count()
    errors: list[BaseException] = []

    def worker(index: int) -> None:
        try:
            coordinator = Coordinator(stores[index], owner_id=f"pod-{index}")
            coordinator.register_handler("test_handler", handler)
            barrier.wait(timeout=60)
            results[index] = coordinator.run(spec)
        except BaseException as exc:  # noqa: BLE001 - surfaced by the assertion
            errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=(index,))
        for index in range(_worker_count())
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=180)

    assert not errors, errors
    assert all(result is not None for result in results)

    outcomes = [result.outcome for result in results]
    assert outcomes.count(Outcome.EXECUTED) == 1, outcomes
    assert outcomes.count(Outcome.SKIPPED_LOCKED) == _worker_count() - 1, outcomes
    assert len(calls) == 1

    assert db_harness.version_count(task_id) == 1
    assert db_harness.lock_count(task_id) == 0


def test_expired_lock_is_taken_over(db_harness):
    store = db_harness.store()
    task_id = db_harness.task_id("expired")
    now = utc(2024, 6, 1, 12, 0, 0)

    store.insert_lock(
        task_id,
        "pod-old",
        now - timedelta(seconds=120),
        now - timedelta(seconds=30),
    )

    observed: dict[str, str | None] = {}

    def handler(config: dict) -> None:
        row = db_harness.lock_row(task_id)
        observed["owner"] = None if row is None else row[0]

    coordinator = Coordinator(store, owner_id="pod-new")
    coordinator.register_handler("test_handler", handler)

    result = coordinator.run(custom_spec(task_id))

    assert result.outcome is Outcome.EXECUTED
    assert result.error is None
    # While executing, the lock row belongs to the new pod ...
    assert observed["owner"] == "pod-new"
    # ... and is released afterwards.
    assert db_harness.lock_count(task_id) == 0
    assert db_harness.version_row(task_id)[0] == "success"


def test_unexpired_lock_is_skipped_locked(db_harness):
    store = db_harness.store()
    task_id = db_harness.task_id("locked")
    now = datetime.now(timezone.utc)

    store.insert_lock(task_id, "pod-old", now, now + timedelta(seconds=300))

    calls: list[dict] = []
    coordinator = Coordinator(store, owner_id="pod-new")
    coordinator.register_handler("test_handler", calls.append)

    result = coordinator.run(custom_spec(task_id))

    assert result.outcome is Outcome.SKIPPED_LOCKED
    assert result.error is None
    assert calls == []
    row = db_harness.lock_row(task_id)
    assert row is not None and row[0] == "pod-old"
    assert db_harness.version_count(task_id) == 0


def test_failure_releases_lock_and_records_failed(db_harness):
    store = db_harness.store()
    task_id = db_harness.task_id("failure")

    def handler(config: dict) -> None:
        raise RuntimeError("boom")

    coordinator = Coordinator(store, owner_id="pod-a")
    coordinator.register_handler("test_handler", handler)

    result = coordinator.run(custom_spec(task_id))

    assert result.outcome is Outcome.FAILED
    assert result.error == "boom"
    assert db_harness.lock_count(task_id) == 0

    assert db_harness.version_count(task_id) == 1
    status, executed_at, owner_id = db_harness.version_row(task_id)
    assert status == "failed"
    assert owner_id == "pod-a"
    assert executed_at is not None


def test_release_does_not_delete_other_owner_lock(db_harness):
    store = db_harness.store()
    task_id = db_harness.task_id("release")
    now = utc(2024, 6, 1, 12, 0, 0)

    store.insert_lock(task_id, "pod-a", now, now + timedelta(seconds=300))
    assert db_harness.steal_lock(task_id, "pod-b") == 1

    Coordinator(store, owner_id="pod-a")._release_lock(task_id)

    row = db_harness.lock_row(task_id)
    assert row is not None, "pod-a must not delete pod-b's lock"
    assert row[0] == "pod-b"

    Coordinator(store, owner_id="pod-b")._release_lock(task_id)
    assert db_harness.lock_count(task_id) == 0


def test_version_bump_is_a_new_batch(db_harness):
    store = db_harness.store()
    task_id = db_harness.task_id("bump")
    calls: list[dict] = []

    coordinator = Coordinator(store, owner_id="pod-a")
    coordinator.register_handler("test_handler", calls.append)

    assert coordinator.run(custom_spec(task_id, version="v1")).outcome is (
        Outcome.EXECUTED
    )
    assert coordinator.run(custom_spec(task_id, version="v2")).outcome is (
        Outcome.EXECUTED
    )
    assert db_harness.version_count(task_id) == 2
    assert db_harness.lock_count(task_id) == 0
    assert len(calls) == 2
