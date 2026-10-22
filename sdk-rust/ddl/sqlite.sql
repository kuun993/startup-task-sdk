-- Startup task coordination schema / SQLite (tests and embedded use)
-- Timestamps are stored as TEXT in the fixed format 'YYYY-MM-DD HH:MM:SS.ffffff' (UTC),
-- so lexicographic order equals chronological order.

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
