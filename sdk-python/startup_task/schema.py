"""``ensure_schema``: create the two tables for the store's dialect.

The DDL text mirrors ``spec/ddl/{postgres,mysql,sqlite}.sql`` verbatim (it is
inlined so an installed wheel does not depend on the repository layout).
"""

from __future__ import annotations

from .store import Dialect, Store, split_sql_script

DDL_POSTGRES = """\
CREATE TABLE IF NOT EXISTS task_execution_lock (
    task_id      VARCHAR(128) PRIMARY KEY,
    owner_id     VARCHAR(128) NOT NULL,
    acquired_at  TIMESTAMP    NOT NULL,
    expires_at   TIMESTAMP    NOT NULL
);

CREATE TABLE IF NOT EXISTS task_version_record (
    task_id      VARCHAR(128) NOT NULL,
    version      VARCHAR(64)  NOT NULL,
    status       VARCHAR(16)  NOT NULL,
    executed_at  TIMESTAMP    NOT NULL,
    owner_id     VARCHAR(128) NOT NULL,
    PRIMARY KEY (task_id, version)
);
"""

DDL_MYSQL = """\
CREATE TABLE IF NOT EXISTS task_execution_lock (
    task_id      VARCHAR(128) NOT NULL,
    owner_id     VARCHAR(128) NOT NULL,
    acquired_at  DATETIME(6)  NOT NULL,
    expires_at   DATETIME(6)  NOT NULL,
    PRIMARY KEY (task_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE IF NOT EXISTS task_version_record (
    task_id      VARCHAR(128) NOT NULL,
    version      VARCHAR(64)  NOT NULL,
    status       VARCHAR(16)  NOT NULL,
    executed_at  DATETIME(6)  NOT NULL,
    owner_id     VARCHAR(128) NOT NULL,
    PRIMARY KEY (task_id, version)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
"""

DDL_SQLITE = """\
CREATE TABLE IF NOT EXISTS task_execution_lock (
    task_id      TEXT PRIMARY KEY,
    owner_id     TEXT NOT NULL,
    acquired_at  TEXT NOT NULL,
    expires_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_version_record (
    task_id      TEXT NOT NULL,
    version      TEXT NOT NULL,
    status       TEXT NOT NULL,
    executed_at  TEXT NOT NULL,
    owner_id     TEXT NOT NULL,
    PRIMARY KEY (task_id, version)
);
"""

DDL_BY_DIALECT = {
    Dialect.POSTGRES: DDL_POSTGRES,
    Dialect.MYSQL: DDL_MYSQL,
    Dialect.SQLITE: DDL_SQLITE,
}


def ddl_for(dialect: Dialect | str) -> str:
    """Return the DDL script for ``dialect``."""
    key = Dialect(dialect) if not isinstance(dialect, Dialect) else dialect
    return DDL_BY_DIALECT[key]


def ensure_schema(store: Store) -> None:
    """Create the lock / version-record tables if they do not exist yet."""
    script = ddl_for(store.dialect)
    store.execute_ddl(script)


def schema_statements(dialect: Dialect | str) -> list[str]:
    """The individual DDL statements, handy for drivers without multi-statement support."""
    return split_sql_script(ddl_for(dialect))


__all__ = [
    "DDL_BY_DIALECT",
    "DDL_MYSQL",
    "DDL_POSTGRES",
    "DDL_SQLITE",
    "ddl_for",
    "ensure_schema",
    "schema_statements",
]
