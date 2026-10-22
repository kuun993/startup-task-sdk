package io.startuptask;

import java.util.Objects;

public record RunResult(String taskId, String version, Outcome outcome, String error, long durationMs) {

    public RunResult {
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(outcome, "outcome");
        if (outcome != Outcome.FAILED) {
            error = null;
        }
    }

    public boolean failed() {
        return outcome == Outcome.FAILED;
    }

    @Override
    public String toString() {
        return "RunResult[taskId=" + taskId
                + ", version=" + version
                + ", outcome=" + outcome
                + ", error=" + error
                + ", durationMs=" + durationMs + "]";
    }
}
