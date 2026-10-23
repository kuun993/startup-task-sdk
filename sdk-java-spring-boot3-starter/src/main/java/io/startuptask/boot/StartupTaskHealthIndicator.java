package io.startuptask.boot;

import io.startuptask.RunResult;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Reports task progress so a readiness probe can wait for it.
 *
 * <p>Not included in the readiness group by default; enable it with:
 * <pre>
 * management.endpoint.health.group.readiness.include: readinessState,startupTask
 * </pre>
 */
public class StartupTaskHealthIndicator implements HealthIndicator {

    private final StartupTaskState state;

    public StartupTaskHealthIndicator(StartupTaskState state) {
        this.state = state;
    }

    @Override
    public Health health() {
        if (!state.isCompleted()) {
            return Health.outOfService()
                    .withDetail("completed", false)
                    .build();
        }
        Health.Builder builder = state.isFailed() ? Health.down() : Health.up();
        if (state.getError() != null) {
            builder.withDetail("error", state.getError());
        }
        List<RunResult> results = state.getResults();
        builder.withDetail("tasks", results.stream().map(RunResult::toString).collect(Collectors.toList()));
        builder.withDetail("failed", state.isFailed());
        return builder.build();
    }
}
