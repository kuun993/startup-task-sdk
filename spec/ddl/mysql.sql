-- Startup task coordination schema / MySQL 5.7+ and 8.x
-- All timestamps are UTC; no NOW() is used (the SDK passes the current time as a bind parameter).

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
