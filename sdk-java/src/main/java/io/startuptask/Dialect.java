package io.startuptask;

import java.sql.SQLException;
import java.util.Locale;

public enum Dialect {

    POSTGRES,
    MYSQL,
    SQLITE;

    static final String Q1 = """
            SELECT 1 FROM task_version_record
             WHERE task_id = ? AND version = ? AND status = 'success'""";

    static final String Q2 = """
            INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at)
            VALUES (?, ?, ?, ?)""";

    static final String Q3 = """
            UPDATE task_execution_lock
               SET owner_id = ?, acquired_at = ?, expires_at = ?
             WHERE task_id = ? AND expires_at < ?""";

    static final String Q5 = """
            DELETE FROM task_execution_lock WHERE task_id = ? AND owner_id = ?""";

    private static final String Q4_ON_CONFLICT = """
            INSERT INTO task_version_record (task_id, version, status, executed_at, owner_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (task_id, version) DO UPDATE
               SET status = excluded.status,
                   executed_at = excluded.executed_at,
                   owner_id = excluded.owner_id""";

    private static final String Q4_ON_DUPLICATE_KEY = """
            INSERT INTO task_version_record (task_id, version, status, executed_at, owner_id)
            VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                status = VALUES(status), executed_at = VALUES(executed_at), owner_id = VALUES(owner_id)""";

    private static final String[] POSTGRES_DDL = {
            """
            CREATE TABLE IF NOT EXISTS task_execution_lock (
                task_id      VARCHAR(128) PRIMARY KEY,
                owner_id     VARCHAR(128) NOT NULL,
                acquired_at  TIMESTAMP    NOT NULL,
                expires_at   TIMESTAMP    NOT NULL
            )""",
            """
            CREATE TABLE IF NOT EXISTS task_version_record (
                task_id      VARCHAR(128) NOT NULL,
                version      VARCHAR(64)  NOT NULL,
                status       VARCHAR(16)  NOT NULL,
                executed_at  TIMESTAMP    NOT NULL,
                owner_id     VARCHAR(128) NOT NULL,
                PRIMARY KEY (task_id, version)
            )"""
    };

    private static final String[] MYSQL_DDL = {
            """
            CREATE TABLE IF NOT EXISTS task_execution_lock (
                task_id      VARCHAR(128) NOT NULL,
                owner_id     VARCHAR(128) NOT NULL,
                acquired_at  DATETIME(6)  NOT NULL,
                expires_at   DATETIME(6)  NOT NULL,
                PRIMARY KEY (task_id)
            ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4""",
            """
            CREATE TABLE IF NOT EXISTS task_version_record (
                task_id      VARCHAR(128) NOT NULL,
                version      VARCHAR(64)  NOT NULL,
                status       VARCHAR(16)  NOT NULL,
                executed_at  DATETIME(6)  NOT NULL,
                owner_id     VARCHAR(128) NOT NULL,
                PRIMARY KEY (task_id, version)
            ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4"""
    };

    private static final String[] SQLITE_DDL = {
            """
            CREATE TABLE IF NOT EXISTS task_execution_lock (
                task_id      TEXT PRIMARY KEY,
                owner_id     TEXT NOT NULL,
                acquired_at  TEXT NOT NULL,
                expires_at   TEXT NOT NULL
            )""",
            """
            CREATE TABLE IF NOT EXISTS task_version_record (
                task_id      TEXT NOT NULL,
                version      TEXT NOT NULL,
                status       TEXT NOT NULL,
                executed_at  TEXT NOT NULL,
                owner_id     TEXT NOT NULL,
                PRIMARY KEY (task_id, version)
            )"""
    };

    String upsertVersionRecord() {
        return this == MYSQL ? Q4_ON_DUPLICATE_KEY : Q4_ON_CONFLICT;
    }

    String[] ddl() {
        return switch (this) {
            case POSTGRES -> POSTGRES_DDL.clone();
            case MYSQL -> MYSQL_DDL.clone();
            case SQLITE -> SQLITE_DDL.clone();
        };
    }

    public boolean isDuplicateKey(SQLException error) {
        return switch (this) {
            case POSTGRES -> "23505".equals(error.getSQLState());
            case MYSQL -> error.getErrorCode() == 1062
                    || ("23000".equals(error.getSQLState()) && containsIgnoreCase(error, "duplicate entry"));
            case SQLITE -> isSqliteDuplicate(error);
        };
    }

    private static boolean isSqliteDuplicate(SQLException error) {
        return "23505".equals(error.getSQLState())
                || error.getErrorCode() == 1555
                || error.getErrorCode() == 2067
                || containsIgnoreCase(error, "unique constraint failed")
                || containsIgnoreCase(error, "primary key");
    }

    private static boolean containsIgnoreCase(SQLException error, String needle) {
        String message = error.getMessage();
        return message != null && message.toLowerCase(Locale.ROOT).contains(needle);
    }
}
