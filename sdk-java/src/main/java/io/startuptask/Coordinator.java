package io.startuptask;

import java.net.InetAddress;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Coordinator {

    private static final Logger LOG = Logs.get(Coordinator.class);
    private static final String DEFAULT_OWNER_ID = buildDefaultOwnerId();
    private static final long MAX_LOCK_LIFETIME_SECONDS = 100L * 365 * 24 * 60 * 60;

    private final Store store;
    private final String ownerId;
    private final Clock clock;
    private final HttpExecutor httpExecutor;
    private final Map<String, TaskHandler> handlers = new ConcurrentHashMap<>();

    public Coordinator(Store store, String ownerId) {
        this(store, ownerId, SystemClock.INSTANCE);
    }

    public Coordinator(Store store, String ownerId, Clock clock) {
        this.store = Objects.requireNonNull(store, "store");
        this.ownerId = ownerId == null || ownerId.isBlank() ? DEFAULT_OWNER_ID : ownerId;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.httpExecutor = new HttpExecutor();
    }

    public static String defaultOwnerId() {
        return DEFAULT_OWNER_ID;
    }

    private static String buildDefaultOwnerId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown-host";
        }
        return host + "/" + UUID.randomUUID().toString().substring(0, 8);
    }

    public String ownerId() {
        return ownerId;
    }

    public void registerHandler(String name, TaskHandler handler) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("handler name must not be empty");
        }
        Objects.requireNonNull(handler, "handler");
        if (handlers.putIfAbsent(name, handler) != null) {
            throw new IllegalStateException("handler \"" + name + "\" is already registered");
        }
    }

    public RunResult run(TaskSpec spec) {
        Objects.requireNonNull(spec, "spec");
        long start = clock.nanoTime();
        Outcome outcome = null;
        String error = null;
        boolean lockHeld = false;

        try {
            if (store.isDone(spec.taskId(), spec.version())) {
                LOG.info(() -> "skip already done task=" + spec.taskId() + " version=" + spec.version());
                outcome = Outcome.SKIPPED_ALREADY_DONE;
            } else {
                Instant now = clock.now();
                Instant expires = now.plusSeconds(effectiveTimeoutSeconds(spec.timeoutSeconds()));
                boolean acquireFailed = false;
                try {
                    Store.LockOutcome acquired = store.acquireLock(spec.taskId(), ownerId, now, expires);
                    switch (acquired) {
                        case ACQUIRED -> {
                            lockHeld = true;
                            LOG.info(() -> "lock acquired task=" + spec.taskId() + " owner=" + ownerId);
                        }
                        case TAKEOVER -> {
                            lockHeld = true;
                            LOG.info(() -> "lock takeover task=" + spec.taskId() + " owner=" + ownerId);
                        }
                        case NOT_ACQUIRED -> {
                        }
                    }
                } catch (Exception e) {
                    acquireFailed = true;
                    error = Values.describe(e);
                    outcome = Outcome.FAILED;
                    LOG.log(Level.WARNING, "lock acquire failed task=" + spec.taskId() + " owner=" + ownerId, e);
                }

                if (!acquireFailed) {
                    if (!lockHeld) {
                        LOG.info(() -> "skip locked task=" + spec.taskId() + " owner=" + ownerId);
                        outcome = Outcome.SKIPPED_LOCKED;
                    } else if (store.isDone(spec.taskId(), spec.version())) {
                        LOG.info(() -> "skip already done task=" + spec.taskId()
                                + " version=" + spec.version() + " (after acquiring lock)");
                        outcome = Outcome.SKIPPED_ALREADY_DONE;
                    } else {
                        executeAndRecordSuccess(spec);
                        outcome = Outcome.EXECUTED;
                    }
                }
            }
        } catch (Exception e) {
            error = Values.describe(e);
            outcome = Outcome.FAILED;
            LOG.log(Level.WARNING, "task failed task=" + spec.taskId() + " owner=" + ownerId, e);
            try {
                store.recordVersion(spec.taskId(), spec.version(), "failed", clock.now(), ownerId);
            } catch (Exception recordError) {
                LOG.log(Level.WARNING, "failed to record failure task=" + spec.taskId() + " owner=" + ownerId,
                        recordError);
            }
        } finally {
            if (lockHeld) {
                try {
                    releaseLock(spec.taskId(), ownerId);
                } catch (Exception e) {
                    LOG.log(Level.WARNING, "release failed task=" + spec.taskId() + " owner=" + ownerId, e);
                }
            }
        }

        long durationMs = Duration.ofNanos(clock.nanoTime() - start).toMillis();
        return new RunResult(spec.taskId(), spec.version(), outcome, error, durationMs);
    }

    public List<RunResult> runAll(List<TaskSpec> specs) {
        Objects.requireNonNull(specs, "specs");
        List<RunResult> results = new ArrayList<>(specs.size());
        for (TaskSpec spec : specs) {
            results.add(run(spec));
        }
        return results;
    }

    void releaseLock(String taskId, String ownerId) throws SQLException {
        store.releaseLock(taskId, ownerId);
    }

    private void execute(TaskSpec spec) throws Exception {
        if (spec.executor() == TaskSpec.Executor.HTTP) {
            httpExecutor.execute(spec);
            return;
        }
        String name = spec.handler();
        TaskHandler handler = name == null ? null : handlers.get(name);
        if (handler == null) {
            throw new TaskExecutionException("handler \"" + name + "\" is not registered");
        }
        handler.handle(spec.handlerConfig());
    }

    private void executeAndRecordSuccess(TaskSpec spec) throws Exception {
        try {
            execute(spec);
            store.recordVersion(spec.taskId(), spec.version(), "success", clock.now(), ownerId);
        } catch (VirtualMachineError fatal) {
            // Under fatal errors such as OOM / StackOverflowError the process is no longer
            // trustworthy: log it and rethrow without writing a version record. The finally block
            // in run() still releases this pod's lock.
            LOG.log(Level.SEVERE, "fatal error while executing task=" + spec.taskId() + " owner=" + ownerId, fatal);
            throw fatal;
        } catch (Throwable error) {
            // Ordinary errors raised during execution (for example a handler throwing AssertionError)
            // converge to FAILED just like exceptions: wrap them in TaskExecutionException so run()'s
            // failure path writes the failed record and releases the lock.
            if (error instanceof Exception e) {
                throw e;
            }
            throw new TaskExecutionException(Values.describe(error), error);
        }
    }

    private static long effectiveTimeoutSeconds(int timeoutSeconds) {
        // Cap the zombie-lock window at 100 years (matching Python/Rust) so an extreme
        // timeout_seconds cannot produce an absurd expiry time.
        return Math.min(timeoutSeconds, MAX_LOCK_LIFETIME_SECONDS);
    }
}
