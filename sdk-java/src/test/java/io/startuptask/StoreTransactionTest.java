package io.startuptask;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class StoreTransactionTest {

    // When a pool disables autocommit the SDK has to commit its own successful work and roll back
    // its failed work. This machine has no PostgreSQL, so SQLite stands in for that path (the PG
    // 25P02 aborted-transaction behaviour cannot be reproduced locally, but the code path -- roll
    // back after the conflict, then run Q3 -- is identical).
    static final class NonAutocommitSqliteStore extends AbstractJdbcStore {

        private final String jdbcUrl;

        NonAutocommitSqliteStore(String jdbcUrl) {
            super(Dialect.SQLITE);
            this.jdbcUrl = jdbcUrl;
        }

        @Override
        protected Connection openConnection() throws SQLException {
            Connection connection = DriverManager.getConnection(jdbcUrl);
            connection.setAutoCommit(false);
            return connection;
        }
    }

    @TempDir
    Path tempDir;

    private String jdbcUrl;
    private NonAutocommitSqliteStore store;

    @BeforeAll
    static void quietLogs() {
        Logger.getLogger("io.startuptask").setLevel(Level.OFF);
    }

    @BeforeEach
    void setUp() throws Exception {
        jdbcUrl = "jdbc:sqlite:" + tempDir.resolve("tx.db").toAbsolutePath();
        store = new NonAutocommitSqliteStore(jdbcUrl);
        Schema.ensure(store);
    }

    @Test
    void schemaAndVersionRecordsAreCommitted() throws SQLException {
        store.recordVersion("task-a", "1", "success", Instant.now(), "pod-a");

        assertEquals("success", scalar("SELECT status FROM task_version_record WHERE task_id = 'task-a'"));
    }

    @Test
    void lockLifecycleIsCommitted() throws SQLException {
        Instant now = Instant.now();
        assertEquals(Store.LockOutcome.ACQUIRED,
                store.acquireLock("task-a", "pod-a", now, now.plusSeconds(300)));
        assertEquals("pod-a", scalar("SELECT owner_id FROM task_execution_lock WHERE task_id = 'task-a'"));

        store.releaseLock("task-a", "pod-a");
        assertNull(scalar("SELECT owner_id FROM task_execution_lock WHERE task_id = 'task-a'"));
    }

    @Test
    void expiredLockTakeoverWorksAfterDuplicateKey() throws SQLException {
        Instant now = Instant.now();
        execute("INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at) VALUES (?, ?, ?, ?)",
                "task-a", "pod-old",
                SqlTypes.formatUtcTimestamp(now.minusSeconds(120)),
                SqlTypes.formatUtcTimestamp(now.minusSeconds(60)));

        assertEquals(Store.LockOutcome.TAKEOVER,
                store.acquireLock("task-a", "pod-new", now, now.plusSeconds(300)));
        assertEquals("pod-new", scalar("SELECT owner_id FROM task_execution_lock WHERE task_id = 'task-a'"));
    }

    @Test
    void versionRecordUpsertIsCommitted() throws SQLException {
        store.recordVersion("task-a", "1", "failed", Instant.now(), "pod-a");
        store.recordVersion("task-a", "1", "success", Instant.now(), "pod-a");

        assertEquals("success", scalar("SELECT status FROM task_version_record WHERE task_id = 'task-a'"));
    }

    private String scalar(String sql) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rows = statement.executeQuery()) {
            return rows.next() ? rows.getString(1) : null;
        }
    }

    private void execute(String sql, String... values) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < values.length; i++) {
                statement.setString(i + 1, values[i]);
            }
            statement.executeUpdate();
        }
    }
}
