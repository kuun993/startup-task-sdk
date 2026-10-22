package io.startuptask;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Shared real-database scenarios for the integration tests: the same acceptance criteria run once
 * on PostgreSQL and once on MySQL(MariaDB). When the DSN environment variable is missing the whole
 * class is skipped, so {@code mvn test} stays green on machines without a database.
 */
abstract class AbstractStoreIntegrationTest {

    static final Calendar UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    JdbcDsn dsn;
    Store store;
    String taskPrefix;

    protected abstract String envName();

    protected abstract String jdbcSubprotocol();

    protected abstract Store createStore(JdbcDsn dsn);

    @BeforeEach
    void openStore() {
        Logger.getLogger("io.startuptask").setLevel(Level.OFF);
        dsn = JdbcDsn.fromEnv(envName(), jdbcSubprotocol());
        assumeTrue(dsn != null, "set " + envName() + " to run " + getClass().getSimpleName());
        store = createStore(dsn);
        try {
            Schema.ensure(store);
        } catch (SQLException e) {
            throw new IllegalStateException("Schema.ensure failed on " + jdbcSubprotocol(), e);
        }
        taskPrefix = "it-" + getClass().getSimpleName().toLowerCase(Locale.ROOT) + "-"
                + UUID.randomUUID().toString().substring(0, 8) + "-";
    }

    @AfterEach
    void deleteOwnRows() {
        if (dsn == null || taskPrefix == null) {
            return;
        }
        try (Connection connection = connect()) {
            deleteLike(connection, "task_execution_lock", taskPrefix + "%");
            deleteLike(connection, "task_version_record", taskPrefix + "%");
        } catch (SQLException ignored) {
            // best effort cleanup; never mask the test result
        }
    }

    private static void deleteLike(Connection connection, String table, String pattern) throws SQLException {
        try (PreparedStatement statement =
                     connection.prepareStatement("DELETE FROM " + table + " WHERE task_id LIKE ?")) {
            statement.setString(1, pattern);
            statement.executeUpdate();
        }
    }

    @Test
    void schemaEnsureIsIdempotentAndTablesAreUsable() throws SQLException {
        Schema.ensure(store);
        String taskId = taskId("schema");
        Instant now = Instant.now();

        assertEquals(Store.LockOutcome.ACQUIRED, store.acquireLock(taskId, "it-pod-a", now, now.plusSeconds(300)));
        assertTrue(!store.isDone(taskId, "1"));
        store.recordVersion(taskId, "1", "success", now, "it-pod-a");
        assertTrue(store.isDone(taskId, "1"));
        assertTrue(!store.isDone(taskId, "2"));
        store.releaseLock(taskId, "it-pod-a");
        assertTrue(lockOwners(taskId).isEmpty());
    }

    @Test
    void firstRunExecuted() throws SQLException {
        TaskSpec spec = customSpec(taskId("first"), "1", "noop");
        AtomicInteger calls = new AtomicInteger();
        Coordinator coordinator = new Coordinator(store, "it-pod-a");
        coordinator.registerHandler("noop", config -> calls.incrementAndGet());

        RunResult result = coordinator.run(spec);

        assertEquals(Outcome.EXECUTED, result.outcome(), result::error);
        assertNull(result.error());
        assertEquals(1, calls.get());
        assertTrue(result.durationMs() >= 0);
        assertTrue(lockOwners(spec.taskId()).isEmpty(), "lock must be released");
        assertEquals("success", stringValue(
                "SELECT status FROM task_version_record WHERE task_id = ? AND version = '1'", spec.taskId()));
        assertEquals("it-pod-a", stringValue(
                "SELECT owner_id FROM task_version_record WHERE task_id = ? AND version = '1'", spec.taskId()));
    }

    @Test
    void secondRunSkippedAlreadyDone() throws SQLException {
        TaskSpec spec = customSpec(taskId("second"), "1", "noop");
        AtomicInteger calls = new AtomicInteger();
        Coordinator first = new Coordinator(store, "it-pod-a");
        first.registerHandler("noop", config -> calls.incrementAndGet());
        assertEquals(Outcome.EXECUTED, first.run(spec).outcome());
        assertTrue(lockOwners(spec.taskId()).isEmpty());

        Coordinator second = new Coordinator(store, "it-pod-b");
        second.registerHandler("noop", config -> calls.incrementAndGet());
        RunResult again = second.run(spec);

        assertEquals(Outcome.SKIPPED_ALREADY_DONE, again.outcome(), again::error);
        assertEquals(1, calls.get());
        assertTrue(lockOwners(spec.taskId()).isEmpty(), "already-done path must not leave a lock");
        assertEquals(1, count("SELECT COUNT(*) FROM task_version_record WHERE task_id = ?", spec.taskId()));
    }

    @Test
    void concurrentOnlyOneExecutes() throws Exception {
        TaskSpec spec = customSpec(taskId("concurrent"), "1", "slow");
        AtomicInteger calls = new AtomicInteger();
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        try {
            List<Future<RunResult>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                String owner = "it-pod-" + i;
                Callable<RunResult> job = () -> {
                    Coordinator coordinator = new Coordinator(store, owner);
                    coordinator.registerHandler("slow", config -> {
                        calls.incrementAndGet();
                        Thread.sleep(300);
                    });
                    barrier.await(30, TimeUnit.SECONDS);
                    return coordinator.run(spec);
                };
                futures.add(pool.submit(job));
            }

            int executed = 0;
            int locked = 0;
            for (Future<RunResult> future : futures) {
                RunResult result = future.get(90, TimeUnit.SECONDS);
                if (result.outcome() == Outcome.EXECUTED) {
                    executed++;
                } else if (result.outcome() == Outcome.SKIPPED_LOCKED) {
                    locked++;
                } else {
                    throw new AssertionError("unexpected outcome: " + result);
                }
            }
            assertEquals(1, executed, "exactly one run must execute");
            assertEquals(threads - 1, locked, "all other runs must be skipped as locked");
            assertEquals(1, calls.get());
            assertTrue(lockOwners(spec.taskId()).isEmpty());
            assertEquals("success", stringValue(
                    "SELECT status FROM task_version_record WHERE task_id = ? AND version = '1'", spec.taskId()));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void expiredLockTakeover() throws Exception {
        TaskSpec spec = customSpec(taskId("takeover"), "1", "observe");
        Instant now = Instant.now();
        execute("INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at) VALUES (?, ?, ?, ?)",
                spec.taskId(), "it-pod-old", now.minusSeconds(120), now.minusSeconds(60));

        AtomicReference<List<String>> observed = new AtomicReference<>();
        Coordinator coordinator = new Coordinator(store, "it-pod-new");
        coordinator.registerHandler("observe", config -> observed.set(lockOwners(spec.taskId())));

        RunResult result = coordinator.run(spec);

        assertEquals(Outcome.EXECUTED, result.outcome(), result::error);
        assertEquals(List.of("it-pod-new"), observed.get(), "lock owner must be the new pod during execution");
        assertTrue(lockOwners(spec.taskId()).isEmpty());
        assertEquals("success", stringValue(
                "SELECT status FROM task_version_record WHERE task_id = ? AND version = '1'", spec.taskId()));
    }

    @Test
    void failureReleasesLockAndRecordsFailed() throws SQLException {
        TaskSpec spec = customSpec(taskId("failure"), "1", "boom");
        Coordinator coordinator = new Coordinator(store, "it-pod-a");
        coordinator.registerHandler("boom", config -> {
            throw new IllegalStateException("handler exploded");
        });

        RunResult result = coordinator.run(spec);

        assertEquals(Outcome.FAILED, result.outcome());
        assertEquals("handler exploded", result.error());
        assertTrue(lockOwners(spec.taskId()).isEmpty(), "lock must be released on failure");
        assertEquals("failed", stringValue(
                "SELECT status FROM task_version_record WHERE task_id = ? AND version = '1'", spec.taskId()));
    }

    @Test
    void releaseDoesNotDeleteOtherOwnerLock() throws Exception {
        String taskId = taskId("release");
        Instant now = Instant.now();
        assertEquals(Store.LockOutcome.ACQUIRED, store.acquireLock(taskId, "it-pod-a", now, now.plusSeconds(300)));
        execute("UPDATE task_execution_lock SET owner_id = ? WHERE task_id = ?", "it-pod-b", taskId);

        Coordinator coordinator = new Coordinator(store, "it-pod-a");
        coordinator.releaseLock(taskId, "it-pod-a");

        assertEquals(List.of("it-pod-b"), lockOwners(taskId));
    }

    String taskId(String suffix) {
        return taskPrefix + suffix;
    }

    static TaskSpec customSpec(String taskId, String version, String handler) {
        return TaskSpecLoader.loadText("""
                task_id: %s
                version: "%s"
                timeout_seconds: 300
                execution:
                  executor: custom
                  handler: %s
                """.formatted(taskId, version, handler));
    }

    Connection connect() throws SQLException {
        Properties properties = new Properties();
        if (dsn.user() != null) {
            properties.setProperty("user", dsn.user());
        }
        if (dsn.password() != null) {
            properties.setProperty("password", dsn.password());
        }
        return DriverManager.getConnection(dsn.jdbcUrl(), properties);
    }

    void execute(String sql, Object... params) throws SQLException {
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, params);
            statement.executeUpdate();
        }
    }

    int count(String sql, Object... params) throws SQLException {
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, params);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? rows.getInt(1) : 0;
            }
        }
    }

    String stringValue(String sql, Object... params) throws SQLException {
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, params);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? rows.getString(1) : null;
            }
        }
    }

    Instant instantValue(String sql, Object... params) throws SQLException {
        try (Connection connection = connect();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, params);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return null;
                }
                Timestamp value = rows.getTimestamp(1, (Calendar) UTC.clone());
                return value == null ? null : value.toInstant();
            }
        }
    }

    List<String> lockOwners(String taskId) throws SQLException {
        List<String> owners = new ArrayList<>();
        try (Connection connection = connect();
             PreparedStatement statement =
                     connection.prepareStatement("SELECT owner_id FROM task_execution_lock WHERE task_id = ?")) {
            statement.setString(1, taskId);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    owners.add(rows.getString(1));
                }
            }
        }
        return owners;
    }

    static void bind(PreparedStatement statement, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            Object param = params[i];
            if (param instanceof Instant instant) {
                statement.setTimestamp(i + 1, Timestamp.from(instant), (Calendar) UTC.clone());
            } else if (param instanceof Timestamp timestamp) {
                statement.setTimestamp(i + 1, timestamp, (Calendar) UTC.clone());
            } else {
                statement.setObject(i + 1, param);
            }
        }
    }
}
