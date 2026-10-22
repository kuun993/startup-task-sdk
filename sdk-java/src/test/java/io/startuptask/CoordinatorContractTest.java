package io.startuptask;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatorContractTest {

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
                timeout_seconds: 30
                execution:
                  executor: custom
                  handler: %s
                """.formatted(taskId, version, handler));
    }

    @Test
    void firstRunExecuted() {
        TaskSpec spec = customSpec("publish-settings", "v1", "publish");
        AtomicInteger calls = new AtomicInteger();
        Coordinator coordinator = new Coordinator(db.store(), "pod-a");
        coordinator.registerHandler("publish", config -> calls.incrementAndGet());

        RunResult result = coordinator.run(spec);

        assertEquals(Outcome.EXECUTED, result.outcome());
        assertEquals("publish-settings", result.taskId());
        assertEquals("v1", result.version());
        assertNull(result.error());
        assertEquals(1, calls.get());
        assertTrue(db.locks().isEmpty(), "lock must be released");

        List<SqliteFixture.VersionRow> versions = db.versions();
        assertEquals(1, versions.size());
        assertEquals("success", versions.get(0).status());
        assertEquals("pod-a", versions.get(0).ownerId());
        assertTrue(versions.get(0).executedAt().matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{6}"),
                "sqlite timestamps are UTC strings with microseconds, got " + versions.get(0).executedAt());
        assertTrue(result.durationMs() >= 0);
        assertTrue(result.toString().contains("EXECUTED"), result.toString());
    }

    @Test
    void secondRunSkippedAlreadyDone() {
        TaskSpec spec = customSpec("publish-settings", "v1", "publish");
        AtomicInteger calls = new AtomicInteger();
        Coordinator first = new Coordinator(db.store(), "pod-a");
        first.registerHandler("publish", config -> calls.incrementAndGet());

        RunResult firstRun = first.run(spec);
        assertEquals(Outcome.EXECUTED, firstRun.outcome());
        assertTrue(db.locks().isEmpty());

        Coordinator second = new Coordinator(db.store(), "pod-b");
        second.registerHandler("publish", config -> calls.incrementAndGet());
        RunResult secondRun = second.run(spec);

        assertEquals(Outcome.SKIPPED_ALREADY_DONE, secondRun.outcome());
        assertNull(secondRun.error());
        assertEquals(1, calls.get(), "handler must not run twice");
        assertTrue(db.locks().isEmpty(), "lock table must stay empty");
        assertEquals(1, db.versions().size());
    }

    @Test
    void alreadyDonePathNeverTouchesTheLockTable() {
        TaskSpec spec = customSpec("already-done-lock", "v1", "publish");
        Coordinator first = new Coordinator(db.store(), "pod-a");
        first.registerHandler("publish", config -> {
        });
        assertEquals(Outcome.EXECUTED, first.run(spec).outcome());

        AtomicInteger acquireCalls = new AtomicInteger();
        Store counting = new DelegatingStore(db.store()) {
            @Override
            public LockOutcome acquireLock(String taskId, String ownerId, Instant acquiredAt, Instant expiresAt)
                    throws SQLException {
                acquireCalls.incrementAndGet();
                return super.acquireLock(taskId, ownerId, acquiredAt, expiresAt);
            }
        };
        Coordinator second = new Coordinator(counting, "pod-b");
        second.registerHandler("publish", config -> {
        });

        assertEquals(Outcome.SKIPPED_ALREADY_DONE, second.run(spec).outcome());
        assertEquals(0, acquireCalls.get(), "step 1 must return before any lock statement is issued");
    }

    @Test
    void concurrentOnlyOneExecutes() throws Exception {
        TaskSpec spec = customSpec("concurrent-task", "v1", "slow");
        AtomicInteger calls = new AtomicInteger();
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        try {
            List<Future<RunResult>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                String owner = "pod-" + i;
                Callable<RunResult> task = () -> {
                    Coordinator coordinator = new Coordinator(db.store(), owner);
                    coordinator.registerHandler("slow", config -> {
                        calls.incrementAndGet();
                        Thread.sleep(200);
                    });
                    barrier.await(30, TimeUnit.SECONDS);
                    return coordinator.run(spec);
                };
                futures.add(pool.submit(task));
            }

            int executed = 0;
            int locked = 0;
            for (Future<RunResult> future : futures) {
                RunResult result = future.get(60, TimeUnit.SECONDS);
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
            assertEquals(1, calls.get(), "handler must run exactly once");
            assertTrue(db.locks().isEmpty());
            assertEquals(1, db.versions().size());
            assertEquals("success", db.versions().get(0).status());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void expiredLockTakeover() {
        TaskSpec spec = customSpec("takeover-task", "v1", "observe");
        Instant now = Instant.now();
        db.insertLock(spec.taskId(), "pod-old", now.minusSeconds(120), now.minusSeconds(60));

        AtomicReference<SqliteFixture.LockRow> observed = new AtomicReference<>();
        Coordinator coordinator = new Coordinator(db.store(), "pod-new");
        coordinator.registerHandler("observe", config -> {
            List<SqliteFixture.LockRow> rows = db.locks();
            assertEquals(1, rows.size(), "lock row must exist while the handler runs");
            observed.set(rows.get(0));
        });

        RunResult result = coordinator.run(spec);

        assertEquals(Outcome.EXECUTED, result.outcome());
        assertNull(result.error());
        assertNotNull(observed.get(), "handler must have observed the taken-over lock");
        assertEquals("pod-new", observed.get().ownerId(), "lock owner must be the new pod during execution");
        assertTrue(db.locks().isEmpty(), "lock must be released after execution");
        assertEquals("success", db.versions().get(0).status());
    }

    @Test
    void failureReleasesLockAndNoSuccess() {
        TaskSpec spec = customSpec("failing-task", "v1", "boom");
        Coordinator coordinator = new Coordinator(db.store(), "pod-a");
        coordinator.registerHandler("boom", config -> {
            throw new IllegalStateException("handler exploded");
        });

        RunResult result = coordinator.run(spec);

        assertEquals(Outcome.FAILED, result.outcome());
        assertEquals("handler exploded", result.error());
        assertTrue(db.locks().isEmpty(), "lock must be released on failure");

        List<SqliteFixture.VersionRow> versions = db.versions();
        assertEquals(1, versions.size());
        assertEquals("failed", versions.get(0).status());
        assertEquals("pod-a", versions.get(0).ownerId());
    }

    @Test
    void releaseDoesNotDeleteOtherOwnerLock() throws SQLException {
        Instant now = Instant.now();
        assertEquals(Store.LockOutcome.ACQUIRED,
                db.store().acquireLock("shared-task", "pod-a", now, now.plusSeconds(300)));
        db.setLockOwner("shared-task", "pod-b");

        Coordinator coordinator = new Coordinator(db.store(), "pod-a");
        coordinator.releaseLock("shared-task", "pod-a");

        List<SqliteFixture.LockRow> rows = db.locks();
        assertEquals(1, rows.size(), "the lock taken over by pod-b must survive");
        assertEquals("pod-b", rows.get(0).ownerId());
    }

    @Test
    void doubleCheckAfterAcquireSkipsAlreadyDoneAndReleasesLock() {
        TaskSpec spec = customSpec("double-check", "v1", "never");
        AtomicInteger doneChecks = new AtomicInteger();
        Store scripted = new DelegatingStore(db.store()) {
            @Override
            public boolean isDone(String taskId, String version) {
                return doneChecks.incrementAndGet() > 1;
            }
        };
        Coordinator coordinator = new Coordinator(scripted, "pod-a");
        coordinator.registerHandler("never", config -> {
            throw new AssertionError("handler must not run when the double check hits");
        });

        RunResult result = coordinator.run(spec);

        assertEquals(Outcome.SKIPPED_ALREADY_DONE, result.outcome());
        assertEquals(2, doneChecks.get(), "step 1 and the post-acquire double check must both query");
        assertTrue(db.locks().isEmpty(), "the acquired lock must be released on the early return path");
    }

    @Test
    void schemaEnsureIsIdempotent() throws SQLException {
        Schema.ensure(db.store());
        Schema.ensure(db.store());
        assertTrue(db.locks().isEmpty());
        assertTrue(db.versions().isEmpty());
    }
}
