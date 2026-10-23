package io.startuptask.boot;

import io.startuptask.RunResult;

import java.time.Instant;
import java.util.List;

/**
 * Outcome of the startup task round. Exposed as a bean so health checks and tests can inspect it.
 */
public class StartupTaskState {

    private volatile boolean completed;
    private volatile boolean failed;
    private volatile List<RunResult> results = List.of();
    private volatile Instant completedAt;
    private volatile String error;

    void record(List<RunResult> results) {
        this.results = List.copyOf(results);
        this.failed = results.stream().anyMatch(RunResult::failed);
        this.completed = true;
        this.completedAt = Instant.now();
    }

    void recordError(Throwable throwable) {
        this.error = throwable.getClass().getSimpleName() + ": " + throwable.getMessage();
        this.failed = true;
        this.completed = true;
        this.completedAt = Instant.now();
    }

    public boolean isCompleted() {
        return completed;
    }

    public boolean isFailed() {
        return failed;
    }

    public List<RunResult> getResults() {
        return results;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public String getError() {
        return error;
    }
}
