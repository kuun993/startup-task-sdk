package io.startuptask.boot;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.Ordered;

/**
 * Runs the startup tasks once the application context is ready: after datasource / Flyway
 * initialisation, before the application reports readiness through the health indicator.
 */
public class StartupTaskApplicationRunner implements ApplicationRunner, Ordered {

    private final StartupTaskExecutor executor;

    public StartupTaskApplicationRunner(StartupTaskExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void run(ApplicationArguments args) {
        executor.execute();
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}
