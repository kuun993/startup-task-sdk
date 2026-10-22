"""startup_task -- cross-language startup task coordination SDK (Python).

Public API (``docs/design.md`` §2):

    Coordinator(store, owner_id=None, clock=None)
    coordinator.register_handler(name, fn)
    load_spec(str_or_path) -> TaskSpec
    coordinator.run(spec) -> RunResult
    coordinator.run_all(specs) -> list[RunResult]
    ensure_schema(store)
"""

from __future__ import annotations

from .coordinator import (
    MAX_LOCK_TIMEOUT_SECONDS,
    Clock,
    Coordinator,
    Outcome,
    RunResult,
    SystemClock,
    default_owner_id,
)
from .executor import (
    ExecutionError,
    Handler,
    HandlerNotRegisteredError,
    HandlerRegistry,
    MissingEnvironmentVariableError,
    substitute_env,
)
from .schema import DDL_BY_DIALECT, ddl_for, ensure_schema, schema_statements
from .spec import (
    DEFAULT_TIMEOUT_SECONDS,
    ExecutionSpec,
    HttpConfig,
    SpecError,
    TaskSpec,
    load_spec,
    parse_spec,
)
from .store import (
    Dialect,
    DuplicateKeyError,
    MysqlStore,
    PostgresStore,
    SqliteStore,
    SqlStore,
    Store,
    StoreError,
    format_sqlite_time,
    parse_mysql_dsn,
    parse_sqlite_time,
)

__version__ = "0.1.0"

__all__ = [
    "Clock",
    "Coordinator",
    "DDL_BY_DIALECT",
    "DEFAULT_TIMEOUT_SECONDS",
    "Dialect",
    "DuplicateKeyError",
    "ExecutionError",
    "ExecutionSpec",
    "Handler",
    "HandlerNotRegisteredError",
    "HandlerRegistry",
    "HttpConfig",
    "MAX_LOCK_TIMEOUT_SECONDS",
    "MissingEnvironmentVariableError",
    "MysqlStore",
    "Outcome",
    "PostgresStore",
    "RunResult",
    "SpecError",
    "SqlStore",
    "SqliteStore",
    "Store",
    "StoreError",
    "SystemClock",
    "TaskSpec",
    "__version__",
    "ddl_for",
    "default_owner_id",
    "ensure_schema",
    "format_sqlite_time",
    "load_spec",
    "parse_mysql_dsn",
    "parse_sqlite_time",
    "parse_spec",
    "schema_statements",
    "substitute_env",
]
