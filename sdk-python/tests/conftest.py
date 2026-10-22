"""Shared fixtures: a real SQLite file database (WAL), never a mocked SQL layer."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from integration_support import harness_context
from startup_task import (
    Coordinator,
    Outcome,
    RunResult,
    SqliteStore,
    TaskSpec,
    ensure_schema,
    parse_spec,
)


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "startup_tasks.db"


@pytest.fixture()
def store(db_path: Path) -> SqliteStore:
    sqlite_store = SqliteStore(db_path)
    ensure_schema(sqlite_store)
    return sqlite_store


@pytest.fixture()
def make_spec():
    """Build a valid custom-executor spec from YAML text (exercises the loader)."""

    def _make(
        task_id: str = "test/task",
        version: str = "v1",
        handler: str = "test_handler",
        config: dict[str, Any] | None = None,
        timeout_seconds: int = 60,
    ) -> TaskSpec:
        lines = [
            f"task_id: {task_id}",
            f'version: "{version}"',
            f"timeout_seconds: {timeout_seconds}",
            "execution:",
            "  executor: custom",
            f"  handler: {handler}",
        ]
        if config:
            lines.append("  config:")
            for key, value in config.items():
                lines.append(f"    {key}: {value}")
        return parse_spec("\n".join(lines) + "\n")

    return _make


@pytest.fixture()
def coordinator(store: SqliteStore) -> Coordinator:
    return Coordinator(store, owner_id="test-owner")


# ---------------------------------------------------------------------------
# Real-database fixtures. They skip when the DSN env var is unset, so the suite
# stays green on machines without PostgreSQL / MySQL.
# ---------------------------------------------------------------------------


@pytest.fixture(params=["pg", "mysql"])
def db_harness(request):
    """Parametrized harness: every shared scenario runs on both dialects."""
    with harness_context(request.param) as harness:
        yield harness


@pytest.fixture()
def pg_harness():
    with harness_context("pg") as harness:
        yield harness


@pytest.fixture()
def mysql_harness():
    with harness_context("mysql") as harness:
        yield harness


__all__ = [
    "Coordinator",
    "Outcome",
    "RunResult",
    "SqliteStore",
    "TaskSpec",
    "db_harness",
    "db_path",
    "ensure_schema",
    "make_spec",
    "mysql_harness",
    "parse_spec",
    "pg_harness",
    "store",
]
