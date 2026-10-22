package io.startuptask;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorMiscTest {

    private SqliteFixture db;

    @BeforeAll
    static void quietLogs() {
        Logger.getLogger("io.startuptask").setLevel(Level.OFF);
    }

    @BeforeEach
    void setUp() {
        db = new SqliteFixture();
    }

    @AfterEach
    void tearDown() {
        db.close();
    }

    private static TaskSpec customSpec(String taskId, String version, String handler) {
        return TaskSpecLoader.loadText("""
                task_id: %s
                version: "%s"
                execution:
                  executor: custom
                  handler: %s
                """.formatted(taskId, version, handler));
    }

    @Test
    void duplicateHandlerRegistrationFails() {
        Coordinator coordinator = new Coordinator(db.store(), "pod-a");
        coordinator.registerHandler("sync", config -> {
        });

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> coordinator.registerHandler("sync", config -> {
                }));
        assertEquals("handler \"sync\" is already registered", error.getMessage());
    }

    @Test
    void unregisteredHandlerFailsWithFixedMessage() {
        TaskSpec spec = customSpec("unknown-handler", "1", "nope");
        Coordinator coordinator = new Coordinator(db.store(), "pod-a");

        RunResult result = coordinator.run(spec);

        assertEquals(Outcome.FAILED, result.outcome());
        assertEquals("handler \"nope\" is not registered", result.error());
        assertTrue(db.locks().isEmpty());
        assertEquals("failed", db.versions().get(0).status());
    }

    @Test
    void handlerAssertionErrorIsConvergedToFailedAndReleasesLock() {
        TaskSpec spec = customSpec("assertion-task", "1", "assert-boom");
        Coordinator coordinator = new Coordinator(db.store(), "pod-a");
        coordinator.registerHandler("assert-boom", config -> {
            throw new AssertionError("expected 1 but was 2");
        });

        RunResult result = coordinator.run(spec);

        assertEquals(Outcome.FAILED, result.outcome());
        assertEquals("expected 1 but was 2", result.error());
        assertTrue(db.locks().isEmpty(), "lock must be released on an ordinary Error");

        List<SqliteFixture.VersionRow> versions = db.versions();
        assertEquals(1, versions.size());
        assertEquals("failed", versions.get(0).status());
        assertEquals("pod-a", versions.get(0).ownerId());
    }

    @Test
    void fatalVmErrorIsRethrownButLockIsStillReleased() {
        TaskSpec spec = customSpec("fatal-task", "1", "fatal");
        Coordinator coordinator = new Coordinator(db.store(), "pod-a");
        coordinator.registerHandler("fatal", config -> {
            throw new StackOverflowError("simulated");
        });

        assertThrows(StackOverflowError.class, () -> coordinator.run(spec));

        assertTrue(db.locks().isEmpty(), "finally must release the lock on fatal errors too");
        assertTrue(db.versions().isEmpty(), "fatal VM errors are rethrown without a version record");
    }

    @Test
    void failedRunDoesNotBlockRetry() {
        TaskSpec spec = customSpec("retryable", "1", "flaky");
        Coordinator first = new Coordinator(db.store(), "pod-a");
        first.registerHandler("flaky", config -> {
            throw new IllegalStateException("first attempt failed");
        });
        assertEquals(Outcome.FAILED, first.run(spec).outcome());
        assertEquals("failed", db.versions().get(0).status());

        Coordinator second = new Coordinator(db.store(), "pod-b");
        second.registerHandler("flaky", config -> {
        });
        RunResult retry = second.run(spec);

        assertEquals(Outcome.EXECUTED, retry.outcome());
        List<SqliteFixture.VersionRow> versions = db.versions();
        assertEquals(1, versions.size());
        assertEquals("success", versions.get(0).status(), "upsert must overwrite the failed record");
        assertEquals("pod-b", versions.get(0).ownerId());
    }

    @Test
    void runAllPreservesOrderAndContinuesAfterFailure() {
        Coordinator coordinator = new Coordinator(db.store(), "pod-a");
        coordinator.registerHandler("ok", config -> {
        });
        coordinator.registerHandler("boom", config -> {
            throw new IllegalStateException("boom");
        });

        List<RunResult> results = coordinator.runAll(List.of(
                customSpec("task-a", "1", "ok"),
                customSpec("task-b", "1", "boom"),
                customSpec("task-c", "1", "ok")));

        assertEquals(3, results.size());
        assertEquals("task-a", results.get(0).taskId());
        assertEquals(Outcome.EXECUTED, results.get(0).outcome());
        assertEquals("task-b", results.get(1).taskId());
        assertEquals(Outcome.FAILED, results.get(1).outcome());
        assertEquals("task-c", results.get(2).taskId());
        assertEquals(Outcome.EXECUTED, results.get(2).outcome());
        assertTrue(db.locks().isEmpty());
    }

    @Test
    void nonDuplicateAcquireErrorFailsWithoutRecordingOrReleasing() {
        TaskSpec spec = customSpec("broken-store", "1", "ok");
        Store broken = new DelegatingStore(db.store()) {
            @Override
            public LockOutcome acquireLock(String taskId, String ownerId, Instant acquiredAt, Instant expiresAt)
                    throws SQLException {
                throw new SQLException("connection reset by peer", "08006");
            }
        };
        Coordinator coordinator = new Coordinator(broken, "pod-a");
        coordinator.registerHandler("ok", config -> {
            throw new AssertionError("handler must not run when the lock cannot be acquired");
        });

        RunResult result = coordinator.run(spec);

        assertEquals(Outcome.FAILED, result.outcome());
        assertEquals("connection reset by peer", result.error());
        assertTrue(db.versions().isEmpty(), "no version record may be written");
        assertTrue(db.locks().isEmpty(), "no lock may be released");
    }

    @Test
    void defaultOwnerIdIsStablePerProcess() {
        Coordinator first = new Coordinator(db.store(), null);
        Coordinator second = new Coordinator(db.store(), "  ");
        assertEquals(Coordinator.defaultOwnerId(), first.ownerId());
        assertEquals(first.ownerId(), second.ownerId());
        assertTrue(first.ownerId().contains("/"), first.ownerId());
    }

    @Test
    void storeConstructorsExposeDialects() {
        assertEquals(Dialect.POSTGRES, new PostgresStore(db.dataSource()).dialect());
        assertEquals(Dialect.MYSQL, new MysqlStore(db.dataSource()).dialect());
        assertEquals(Dialect.SQLITE, db.store().dialect());
        assertEquals(Dialect.POSTGRES, new PostgresStore(db.jdbcUrl(), "user", "secret").dialect());
    }
}
