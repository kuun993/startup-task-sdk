"""The coordination algorithm of ``docs/design.md`` §5."""

from __future__ import annotations

import logging
import socket
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Iterable, Protocol

from .executor import Handler, HandlerRegistry, execute
from .spec import TaskSpec
from .store import STATUS_FAILED, STATUS_SUCCESS, DuplicateKeyError, Store

logger = logging.getLogger(__name__)

#: Upper bound for a lock lease (100 years). ``timeout_seconds`` is only
#: validated as ``> 0``, so an absurd value must not overflow ``timedelta`` and
#: turn a valid spec into ``FAILED``. Matches the Rust implementation.
MAX_LOCK_TIMEOUT_SECONDS = 100 * 365 * 24 * 60 * 60


class Outcome(str, Enum):
    """§3 outcome of a single ``run``."""

    EXECUTED = "EXECUTED"
    SKIPPED_ALREADY_DONE = "SKIPPED_ALREADY_DONE"
    SKIPPED_LOCKED = "SKIPPED_LOCKED"
    FAILED = "FAILED"

    def __str__(self) -> str:
        return self.value

    def __format__(self, format_spec: str) -> str:
        return format(self.value, format_spec)

    def to_string(self) -> str:
        """Upper-case constant name, identical across the four languages."""
        return self.value


@dataclass(frozen=True)
class RunResult:
    """§2 result of a single ``run``."""

    task_id: str
    version: str
    outcome: Outcome
    error: str | None = None
    duration_ms: int = 0


class Clock(Protocol):
    """Injectable time source (UTC wall clock + monotonic clock)."""

    def now_utc(self) -> datetime: ...

    def monotonic(self) -> float: ...


class SystemClock:
    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

    def monotonic(self) -> float:
        return time.monotonic()


_PROCESS_OWNER_ID: str | None = None


def default_owner_id() -> str:
    """``<hostname>/<uuid8>``, fixed for the lifetime of the process."""
    global _PROCESS_OWNER_ID
    if _PROCESS_OWNER_ID is None:
        _PROCESS_OWNER_ID = f"{socket.gethostname()}/{uuid.uuid4().hex[:8]}"
    return _PROCESS_OWNER_ID


class _RunState:
    __slots__ = ("outcome", "error", "lock_held", "phase")

    def __init__(self) -> None:
        self.outcome: Outcome | None = None
        self.error: str | None = None
        self.lock_held = False
        #: ``check`` (Q1) -> ``acquire`` (Q2/Q3) -> ``execute`` (handler + Q4)
        self.phase = "check"


class Coordinator:
    """Runs task specs at most once per ``(task_id, version)``."""

    def __init__(
        self,
        store: Store,
        owner_id: str | None = None,
        clock: Clock | None = None,
        *,
        handlers: HandlerRegistry | None = None,
    ) -> None:
        if store is None:
            raise ValueError("store is required")
        self._store = store
        self._owner_id = owner_id or default_owner_id()
        self._clock = clock if clock is not None else SystemClock()
        monotonic = getattr(self._clock, "monotonic", None)
        self._monotonic = monotonic if callable(monotonic) else time.monotonic
        self._handlers = handlers if handlers is not None else HandlerRegistry()

    @property
    def store(self) -> Store:
        return self._store

    @property
    def owner_id(self) -> str:
        return self._owner_id

    @property
    def handlers(self) -> HandlerRegistry:
        return self._handlers

    def register_handler(self, name: str, fn: Handler) -> None:
        """Register a ``custom`` executor handler; duplicate names raise ``ValueError``."""
        self._handlers.register(name, fn)

    def run(self, spec: TaskSpec) -> RunResult:
        """Execute ``spec`` once, converging every failure into the ``RunResult``."""
        started = self._monotonic()
        state = _RunState()
        try:
            self._attempt(spec, state)
        except BaseException as exc:  # noqa: BLE001 - §7: converge, never raise
            if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
                raise
            state.error = _message(exc)
            state.outcome = Outcome.FAILED
            logger.warning(
                "task failed task_id=%s version=%s owner_id=%s phase=%s error=%s",
                spec.task_id,
                spec.version,
                self._owner_id,
                state.phase,
                state.error,
            )
            if state.phase != "acquire":
                self._record_failure(spec)
            else:
                logger.warning(
                    "lock phase failure task_id=%s version=%s: "
                    "no version record written, lock not released",
                    spec.task_id,
                    spec.version,
                )
        finally:
            if state.lock_held:
                self._release_lock(spec.task_id)
            duration_ms = int(round((self._monotonic() - started) * 1000))

        return RunResult(
            task_id=spec.task_id,
            version=spec.version,
            outcome=state.outcome if state.outcome is not None else Outcome.FAILED,
            error=state.error,
            duration_ms=duration_ms,
        )

    def run_all(self, specs: Iterable[TaskSpec]) -> list[RunResult]:
        """Run specs serially; a failure never aborts the remaining specs."""
        return [self.run(spec) for spec in specs]

    # ---------------------------------------------------------------- steps

    def _attempt(self, spec: TaskSpec, state: _RunState) -> None:
        # Step 1: a successful (task_id, version) record means somebody is done.
        state.phase = "check"
        if self._store.exists_success(spec.task_id, spec.version):
            logger.info(
                "skip already done task_id=%s version=%s owner_id=%s",
                spec.task_id,
                spec.version,
                self._owner_id,
            )
            state.outcome = Outcome.SKIPPED_ALREADY_DONE
            return

        # Step 2: two atomic paths, never SELECT-then-INSERT.
        state.phase = "acquire"
        now = self._clock.now_utc()
        lease_seconds = min(spec.timeout_seconds, MAX_LOCK_TIMEOUT_SECONDS)
        expires_at = now + timedelta(seconds=lease_seconds)
        try:
            self._store.insert_lock(spec.task_id, self._owner_id, now, expires_at)
            state.lock_held = True
            logger.info(
                "lock acquired task_id=%s version=%s owner_id=%s expires_at=%s",
                spec.task_id,
                spec.version,
                self._owner_id,
                expires_at.isoformat(),
            )
        except DuplicateKeyError:
            rows = self._store.takeover_lock(
                self._owner_id, now, expires_at, spec.task_id, now
            )
            if rows == 1:
                state.lock_held = True
                logger.info(
                    "lock takeover task_id=%s version=%s owner_id=%s expires_at=%s",
                    spec.task_id,
                    spec.version,
                    self._owner_id,
                    expires_at.isoformat(),
                )

        if not state.lock_held:
            logger.info(
                "skip locked task_id=%s version=%s owner_id=%s",
                spec.task_id,
                spec.version,
                self._owner_id,
            )
            state.outcome = Outcome.SKIPPED_LOCKED
            return

        # Step 3: double check -- a takeover may have run while the previous
        # owner finished and recorded success.
        state.phase = "execute"
        if self._store.exists_success(spec.task_id, spec.version):
            logger.info(
                "skip already done task_id=%s version=%s owner_id=%s (after acquire)",
                spec.task_id,
                spec.version,
                self._owner_id,
            )
            state.outcome = Outcome.SKIPPED_ALREADY_DONE
            return

        # Step 4: execute, then record success (4a).
        execute(spec, self._handlers)
        self._store.upsert_version(
            spec.task_id,
            spec.version,
            STATUS_SUCCESS,
            self._clock.now_utc(),
            self._owner_id,
        )
        state.outcome = Outcome.EXECUTED

    def _record_failure(self, spec: TaskSpec) -> None:
        try:
            self._store.upsert_version(
                spec.task_id,
                spec.version,
                STATUS_FAILED,
                self._clock.now_utc(),
                self._owner_id,
            )
        except Exception as exc:  # noqa: BLE001 - §5: log only, never change outcome
            logger.warning(
                "failed to record failure task_id=%s version=%s: %s",
                spec.task_id,
                spec.version,
                exc,
            )

    def _release_lock(self, task_id: str) -> None:
        """Q5: delete our own lock. Failures are logged and never change the outcome."""
        try:
            self._store.release_lock(task_id, self._owner_id)
            logger.debug(
                "lock released task_id=%s owner_id=%s", task_id, self._owner_id
            )
        except Exception as exc:  # noqa: BLE001 - §5/§7: log only
            logger.warning(
                "release failed task_id=%s owner_id=%s: %s",
                task_id,
                self._owner_id,
                exc,
            )


def _message(exc: BaseException) -> str:
    text = str(exc)
    if text:
        return text
    return type(exc).__name__


__all__ = [
    "Clock",
    "Coordinator",
    "MAX_LOCK_TIMEOUT_SECONDS",
    "Outcome",
    "RunResult",
    "SystemClock",
    "default_owner_id",
]
