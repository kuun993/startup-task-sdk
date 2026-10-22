"""design.md §10 scenarios 1-6, plus the surrounding behaviour they imply."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from helpers import (
    insert_raw_lock,
    lock_owner,
    lock_rows,
    new_store,
    steal_lock,
    version_rows,
)
from startup_task import (
    MAX_LOCK_TIMEOUT_SECONDS,
    Coordinator,
    Dialect,
    Outcome,
    SqliteStore,
    StoreError,
    ensure_schema,
    format_sqlite_time,
    parse_spec,
)

TASK_ID = "test/task"
VERSION = "v1"


# --------------------------------------------------------------------- §10.1


def test_first_run_executed(store, coordinator, make_spec, db_path):
    calls: list[dict] = []
    coordinator.register_handler("test_handler", calls.append)

    result = coordinator.run(make_spec())

    assert result.outcome is Outcome.EXECUTED
    assert result.error is None
    assert result.task_id == TASK_ID
    assert result.version == VERSION
    assert result.duration_ms >= 0
    assert calls == [{}]

    rows = version_rows(db_path)
    assert len(rows) == 1
    assert rows[0][:3] == (TASK_ID, VERSION, "success")
    assert rows[0][4] == "test-owner"
    assert lock_rows(db_path) == []


# --------------------------------------------------------------------- §10.2


def test_second_run_skipped_already_done(store, coordinator, make_spec, db_path):
    calls: list[dict] = []
    coordinator.register_handler("test_handler", calls.append)
    spec = make_spec()

    assert coordinator.run(spec).outcome is Outcome.EXECUTED
    assert lock_rows(db_path) == []

    result = coordinator.run(spec)

    assert result.outcome is Outcome.SKIPPED_ALREADY_DONE
    assert result.error is None
    assert calls == [{}]
    # The lock table was never written: empty before and after the second run.
    assert lock_rows(db_path) == []

    other = _new_pod(db_path, "other-pod", calls.append)
    assert other.run(spec).outcome is Outcome.SKIPPED_ALREADY_DONE
    assert lock_rows(db_path) == []


# --------------------------------------------------------------------- §10.3


def test_concurrent_only_one_executes(db_path, make_spec):
    ensure_schema(new_store(db_path))

    calls: list[dict] = []
    calls_lock = threading.Lock()
    barrier = threading.Barrier(8)

    def handler(config: dict) -> None:
        with calls_lock:
            calls.append(config)
        time.sleep(1.0)  # keep the lock held while the others contend

    spec = make_spec(timeout_seconds=300)

    # Create the stores (== their sqlite connections) up front, in order, so the
    # WAL switch is not itself raced; the statement execution below is concurrent.
    stores = [new_store(db_path) for _ in range(8)]
    for candidate in stores:
        candidate.exists_success(TASK_ID, VERSION)

    results: list = [None] * 8
    errors: list[BaseException] = []

    def worker(index: int) -> None:
        try:
            coordinator = Coordinator(stores[index], owner_id=f"pod-{index}")
            coordinator.register_handler("test_handler", handler)
            barrier.wait(timeout=30)
            results[index] = coordinator.run(spec)
        except BaseException as exc:  # noqa: BLE001 - surface in the assertion
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)

    assert not errors, errors
    assert all(result is not None for result in results)

    outcomes = [result.outcome for result in results]
    assert outcomes.count(Outcome.EXECUTED) == 1, outcomes
    assert outcomes.count(Outcome.SKIPPED_LOCKED) == 7, outcomes
    assert len(calls) == 1

    rows = version_rows(db_path)
    assert len(rows) == 1 and rows[0][2] == "success"
    assert lock_rows(db_path) == []


# --------------------------------------------------------------------- §10.4


def test_expired_lock_takeover(db_path, make_spec):
    store = new_store(db_path)
    ensure_schema(store)
    insert_raw_lock(db_path, TASK_ID, "pod-old", expires_in_seconds=-30)

    observed: dict[str, str | None] = {}

    def handler(config: dict) -> None:
        observed["owner"] = lock_owner(db_path, TASK_ID)

    coordinator = _new_pod(db_path, "pod-new", handler)

    result = coordinator.run(make_spec())

    assert result.outcome is Outcome.EXECUTED
    assert result.error is None
    # While executing, the lock belongs to the new pod ...
    assert observed["owner"] == "pod-new"
    # ... and is released afterwards.
    assert lock_rows(db_path) == []
    assert version_rows(db_path)[0][:3] == (TASK_ID, VERSION, "success")


def test_unexpired_lock_is_skipped_locked(db_path, make_spec):
    store = new_store(db_path)
    ensure_schema(store)
    insert_raw_lock(db_path, TASK_ID, "pod-old", expires_in_seconds=120)

    calls: list[dict] = []
    coordinator = _new_pod(db_path, "pod-new", calls.append)

    result = coordinator.run(make_spec())

    assert result.outcome is Outcome.SKIPPED_LOCKED
    assert calls == []
    assert lock_owner(db_path, TASK_ID) == "pod-old"
    assert version_rows(db_path) == []


# --------------------------------------------------------------------- §10.5


def test_failure_releases_lock_and_no_success(store, coordinator, make_spec, db_path):
    def handler(config: dict) -> None:
        raise RuntimeError("boom")

    coordinator.register_handler("test_handler", handler)

    result = coordinator.run(make_spec())

    assert result.outcome is Outcome.FAILED
    assert result.error == "boom"
    assert lock_rows(db_path) == []

    rows = version_rows(db_path)
    assert len(rows) == 1
    assert rows[0][:3] == (TASK_ID, VERSION, "failed")
    assert rows[0][2] != "success"

    # A failed record must not block a later retry (§5 point 1).
    retry = _new_pod(db_path, "retry-pod", lambda config: None)
    assert retry.run(make_spec()).outcome is Outcome.EXECUTED
    assert version_rows(db_path)[0][:3] == (TASK_ID, VERSION, "success")


# --------------------------------------------------------------------- §10.6


def test_release_does_not_delete_other_owner_lock(store, db_path, make_spec):
    now = datetime.now(timezone.utc)
    store.insert_lock(TASK_ID, "pod-a", now, now + timedelta(seconds=60))
    steal_lock(db_path, TASK_ID, "pod-b")

    coordinator_a = Coordinator(store, owner_id="pod-a")
    coordinator_a._release_lock(TASK_ID)

    assert lock_owner(db_path, TASK_ID) == "pod-b"

    coordinator_b = Coordinator(store, owner_id="pod-b")
    coordinator_b._release_lock(TASK_ID)

    assert lock_rows(db_path) == []


def test_release_failure_does_not_change_outcome(db_path, make_spec, caplog):
    broken = _WrappedStore(new_store(db_path))
    ensure_schema(broken)
    broken.release_lock = _raise_release_failure  # type: ignore[method-assign]

    coordinator = Coordinator(broken, owner_id="pod-a")
    coordinator.register_handler("test_handler", lambda config: None)

    with caplog.at_level("WARNING", logger="startup_task.coordinator"):
        result = coordinator.run(make_spec())

    assert result.outcome is Outcome.EXECUTED
    assert "release failed" in caplog.text
    assert lock_owner(db_path, TASK_ID) == "pod-a"


# ------------------------------------------------- other normative behaviours


def test_double_check_after_acquire_returns_skipped_already_done(db_path, make_spec):
    inner = new_store(db_path)
    ensure_schema(inner)
    inner.upsert_version(
        TASK_ID, VERSION, "success", datetime.now(timezone.utc), "pod-x"
    )

    calls: list[dict] = []
    coordinator = Coordinator(
        _FirstCheckMisses(inner, inner), owner_id="pod-y"
    )
    coordinator.register_handler("test_handler", calls.append)

    result = coordinator.run(make_spec())

    assert result.outcome is Outcome.SKIPPED_ALREADY_DONE
    assert result.outcome is not Outcome.EXECUTED
    assert result.outcome is not Outcome.SKIPPED_LOCKED
    assert calls == []
    # The lock taken by pod-y is released on the early return path.
    assert lock_rows(db_path) == []


def test_lock_phase_error_fails_without_version_record_or_release(db_path, make_spec):
    broken = _WrappedStore(new_store(db_path))
    ensure_schema(broken)
    broken.insert_lock = _raise_store_error  # type: ignore[method-assign]

    calls: list[dict] = []
    coordinator = Coordinator(broken, owner_id="pod-a")
    coordinator.register_handler("test_handler", calls.append)

    result = coordinator.run(make_spec())

    assert result.outcome is Outcome.FAILED
    assert "disk on fire" in (result.error or "")
    assert calls == []
    assert version_rows(db_path) == []
    assert lock_rows(db_path) == []


def test_precheck_store_error_is_failed(db_path, make_spec):
    broken = _WrappedStore(new_store(db_path))
    ensure_schema(broken)

    def explode(*args, **kwargs):
        raise StoreError("version table unreachable")

    broken.exists_success = explode  # type: ignore[method-assign]

    coordinator = Coordinator(broken, owner_id="pod-a")
    coordinator.register_handler("test_handler", lambda config: None)

    result = coordinator.run(make_spec())

    assert result.outcome is Outcome.FAILED
    assert "version table unreachable" in (result.error or "")
    # The lock phase was never reached.
    assert lock_rows(db_path) == []


def test_execute_phase_store_error_is_failed(db_path, make_spec):
    broken = _WrappedStore(new_store(db_path))
    ensure_schema(broken)

    def explode(*args, **kwargs):
        raise StoreError("version table unavailable")

    broken.upsert_version = explode  # type: ignore[method-assign]

    coordinator = Coordinator(broken, owner_id="pod-a")
    coordinator.register_handler("test_handler", lambda config: None)

    result = coordinator.run(make_spec())

    assert result.outcome is Outcome.FAILED
    assert "version table unavailable" in (result.error or "")
    assert lock_rows(db_path) == []


def test_missing_handler_fails_with_contract_message(store, coordinator, make_spec):
    result = coordinator.run(make_spec(handler="nobody"))

    assert result.outcome is Outcome.FAILED
    assert result.error == 'handler "nobody" is not registered'
    assert lock_rows(store.path) == []


def test_handler_receives_config_copy(store, coordinator, make_spec):
    received: list[dict] = []

    def handler(config: dict) -> None:
        received.append(dict(config))
        config["batch_size"] = 999
        config["extra"] = True

    coordinator.register_handler("test_handler", handler)
    spec = make_spec(config={"batch_size": 100, "dry_run": False})

    assert coordinator.run(spec).outcome is Outcome.EXECUTED
    assert received == [{"batch_size": 100, "dry_run": False}]
    assert spec.execution.config == {"batch_size": 100, "dry_run": False}
    assert "handler" not in received[0]


def test_register_handler_rejects_duplicates(coordinator):
    coordinator.register_handler("dup", lambda config: None)
    with pytest.raises(ValueError) as excinfo:
        coordinator.register_handler("dup", lambda config: None)
    assert "dup" in str(excinfo.value)


def test_run_all_keeps_order_and_continues_after_failure(store, coordinator, make_spec):
    def failing(config: dict) -> None:
        raise RuntimeError("nope")

    coordinator.register_handler("failing", failing)
    coordinator.register_handler("ok", lambda config: None)

    specs = [
        make_spec(task_id="a/ok-1", handler="ok"),
        make_spec(task_id="b/broken", handler="failing"),
        make_spec(task_id="c/ok-2", handler="ok"),
    ]

    results = coordinator.run_all(specs)

    assert [result.task_id for result in results] == [
        "a/ok-1",
        "b/broken",
        "c/ok-2",
    ]
    assert [result.outcome for result in results] == [
        Outcome.EXECUTED,
        Outcome.FAILED,
        Outcome.EXECUTED,
    ]


def test_clock_is_used_for_expiry_and_duration(store, db_path, make_spec):
    fixed_now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    clock = _FakeClock(fixed_now)

    observed: dict[str, str] = {}

    def handler(config: dict) -> None:
        row = lock_rows(db_path)[0]
        observed["acquired_at"] = row[2]
        observed["expires_at"] = row[3]
        clock.advance(1.5)

    coordinator = Coordinator(store, owner_id="pod-a", clock=clock)
    coordinator.register_handler("test_handler", handler)

    result = coordinator.run(make_spec(timeout_seconds=42))

    assert result.outcome is Outcome.EXECUTED
    assert result.duration_ms == 1500
    assert observed["acquired_at"] == "2024-06-01 12:00:00.000000"
    assert observed["expires_at"] == "2024-06-01 12:00:42.000000"


def test_extreme_timeout_is_clamped_for_lock_expiry(store, db_path, make_spec):
    fixed_now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    clock = _FakeClock(fixed_now)
    observed: dict[str, str] = {}

    def handler(config: dict) -> None:
        observed["expires_at"] = lock_rows(db_path)[0][3]

    coordinator = Coordinator(store, owner_id="pod-a", clock=clock)
    coordinator.register_handler("test_handler", handler)

    spec = make_spec(timeout_seconds=10**18)
    assert spec.timeout_seconds == 10**18  # validation stays "> 0"

    result = coordinator.run(spec)

    assert result.outcome is Outcome.EXECUTED
    expected = fixed_now + timedelta(seconds=MAX_LOCK_TIMEOUT_SECONDS)
    assert observed["expires_at"] == format_sqlite_time(expected)


def test_default_owner_id_shape(store):
    coordinator = Coordinator(store)
    hostname, _, suffix = coordinator.owner_id.partition("/")
    assert hostname
    assert len(suffix) == 8
    assert coordinator.owner_id == Coordinator(store).owner_id


def test_sqlite_dialect_and_time_format(store, db_path, make_spec):
    assert store.dialect is Dialect.SQLITE
    coordinator = Coordinator(store, owner_id="pod-a")
    coordinator.register_handler("test_handler", lambda config: None)
    assert coordinator.run(make_spec()).outcome is Outcome.EXECUTED

    executed_at = version_rows(db_path)[0][3]
    assert len(executed_at) == len("2024-06-01 12:00:00.000000")
    assert executed_at[10] == " " and executed_at[19] == "."


# ---------------------------------------------------------------- test doubles


def _new_pod(db_path, owner_id: str, handler) -> Coordinator:
    store = new_store(db_path)
    coordinator = Coordinator(store, owner_id=owner_id)
    coordinator.register_handler("test_handler", handler)
    return coordinator


class _WrappedStore:
    """Delegating store wrapper used to inject store-level failures."""

    def __init__(self, inner) -> None:
        self._inner = inner
        self.dialect = inner.dialect

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _FirstCheckMisses(_WrappedStore):
    """Forces the step-1 Q1 to miss, so the step-3 double check is exercised."""

    def __init__(self, inner, real) -> None:
        super().__init__(inner)
        self._real = real
        self._calls = 0

    def exists_success(self, task_id: str, version: str) -> bool:
        self._calls += 1
        if self._calls == 1:
            return False
        return self._real.exists_success(task_id, version)


class _FakeClock:
    def __init__(self, now: datetime) -> None:
        self._now = now
        self._monotonic = 100.0

    def now_utc(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._monotonic

    def advance(self, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)
        self._monotonic += seconds


def _raise_release_failure(task_id, owner_id):
    raise StoreError("release boom")


def _raise_store_error(*args, **kwargs):
    raise StoreError("disk on fire")
