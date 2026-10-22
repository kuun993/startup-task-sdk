package io.startuptask;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IntegrationPgTest extends AbstractStoreIntegrationTest {

    @Override
    protected String envName() {
        return "STARTUP_TASK_PG_DSN";
    }

    @Override
    protected String jdbcSubprotocol() {
        return "postgresql";
    }

    @Override
    protected Store createStore(JdbcDsn dsn) {
        return new PostgresStore(dsn.jdbcUrl(), dsn.user(), dsn.password());
    }

    @Test
    void schemaUsesVarcharAndTimestampColumns() throws SQLException {
        assertEquals(2, count("""
                SELECT COUNT(*) FROM information_schema.tables
                 WHERE table_schema = current_schema()
                   AND table_name IN ('task_execution_lock', 'task_version_record')"""));
        assertEquals("timestamp without time zone", stringValue("""
                SELECT data_type FROM information_schema.columns
                 WHERE table_schema = current_schema()
                   AND table_name = 'task_execution_lock' AND column_name = 'acquired_at'"""));
        assertEquals("timestamp without time zone", stringValue("""
                SELECT data_type FROM information_schema.columns
                 WHERE table_schema = current_schema()
                   AND table_name = 'task_version_record' AND column_name = 'executed_at'"""));
        assertEquals(128, count("""
                SELECT character_maximum_length FROM information_schema.columns
                 WHERE table_schema = current_schema()
                   AND table_name = 'task_execution_lock' AND column_name = 'owner_id'"""));
        assertEquals("character varying", stringValue("""
                SELECT data_type FROM information_schema.columns
                 WHERE table_schema = current_schema()
                   AND table_name = 'task_version_record' AND column_name = 'version'"""));
    }

    @Test
    void timestampsAreStoredInUtcRegardlessOfJvmDefaultTimeZone() throws SQLException {
        TaskSpec spec = customSpec(taskId("utc"), "1", "noop");
        Instant acquiredAt = Instant.parse("2024-11-01T03:04:05.123456Z");
        Instant expiresAt = acquiredAt.plusSeconds(300);
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));

            assertEquals(Store.LockOutcome.ACQUIRED,
                    store.acquireLock(spec.taskId(), "it-pod-a", acquiredAt, expiresAt));

            assertEquals(acquiredAt, instantValue(
                    "SELECT acquired_at FROM task_execution_lock WHERE task_id = ?", spec.taskId()));
            assertEquals(expiresAt, instantValue(
                    "SELECT expires_at FROM task_execution_lock WHERE task_id = ?", spec.taskId()));

            store.recordVersion(spec.taskId(), "1", "success", acquiredAt, "it-pod-a");
            assertEquals(acquiredAt, instantValue(
                    "SELECT executed_at FROM task_version_record WHERE task_id = ?", spec.taskId()));
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    void nonAutocommitConnectionCanTakeOverExpiredLockWithout25P02() throws Exception {
        PostgresStore nonAutocommitStore = new PostgresStore(new NonAutocommitDataSource(dsn));
        TaskSpec spec = customSpec(taskId("nonauto"), "1", "noop");
        Instant now = Instant.now();
        assertEquals(Store.LockOutcome.ACQUIRED,
                store.acquireLock(spec.taskId(), "it-pod-a", now, now.plusSeconds(3600)));

        AtomicInteger calls = new AtomicInteger();
        Coordinator podB = new Coordinator(nonAutocommitStore, "it-pod-b");
        podB.registerHandler("noop", config -> calls.incrementAndGet());

        RunResult locked = podB.run(spec);
        assertEquals(Outcome.SKIPPED_LOCKED, locked.outcome(), locked::error);
        assertNull(locked.error(), "duplicate key + Q3 must not surface as a store error (25P02)");
        assertEquals(0, calls.get());
        assertEquals(List.of("it-pod-a"), lockOwners(spec.taskId()));

        execute("UPDATE task_execution_lock SET expires_at = ? WHERE task_id = ?",
                now.minusSeconds(60), spec.taskId());

        RunResult taken = podB.run(spec);
        assertEquals(Outcome.EXECUTED, taken.outcome(), taken::error);
        assertNull(taken.error());
        assertEquals(1, calls.get());
        assertTrue(lockOwners(spec.taskId()).isEmpty(), "lock must be released after takeover");
        assertEquals("success", stringValue(
                "SELECT status FROM task_version_record WHERE task_id = ? AND version = '1'", spec.taskId()));
    }

    /** Simulates a pool with autocommit disabled: every getConnection() returns autoCommit=false. */
    static final class NonAutocommitDataSource implements DataSource {

        private final SimpleDataSource delegate;

        NonAutocommitDataSource(JdbcDsn dsn) {
            this.delegate = new SimpleDataSource(dsn.jdbcUrl(), dsn.user(), dsn.password());
        }

        @Override
        public Connection getConnection() throws SQLException {
            Connection connection = delegate.getConnection();
            connection.setAutoCommit(false);
            return connection;
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            Connection connection = delegate.getConnection(username, password);
            connection.setAutoCommit(false);
            return connection;
        }

        @Override
        public PrintWriter getLogWriter() {
            return delegate.getLogWriter();
        }

        @Override
        public void setLogWriter(PrintWriter out) {
            delegate.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) {
            delegate.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() {
            return delegate.getLoginTimeout();
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return delegate.getParentLogger();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return delegate.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return delegate.isWrapperFor(iface);
        }
    }
}
