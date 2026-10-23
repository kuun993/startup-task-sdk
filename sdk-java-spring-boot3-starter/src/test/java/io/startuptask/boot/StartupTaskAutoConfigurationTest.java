package io.startuptask.boot;

import io.startuptask.Outcome;
import io.startuptask.RunResult;
import io.startuptask.TaskHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StartupTaskAutoConfigurationTest {

    @TempDir
    Path temp;

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(StartupTaskAutoConfiguration.class))
                .withUserConfiguration(TestConfiguration.class)
                .withPropertyValues(
                        "test.db=jdbc:sqlite:" + temp.resolve("tasks.db").toString().replace('\\', '/'),
                        "test.token=secret-token");
    }

    private ApplicationContextRunner enabled(String specs) {
        return runner().withPropertyValues(
                "startup-task.enabled=true",
                "startup-task.initialize-schema=true",
                "startup-task.specs=" + specs);
    }

    @Test
    void isInertWithoutTheEnabledFlag() {
        runner().run(context -> {
            assertThat(context).doesNotHaveBean(StartupTaskExecutor.class);
            assertThat(context).doesNotHaveBean(StartupTaskApplicationRunner.class);
        });
    }

    @Test
    void executesTasksAndResolvesSpringPlaceholders() {
        enabled("classpath:startup/warm-up.yaml").run(context -> {
            StartupTaskExecutor executor = context.getBean(StartupTaskExecutor.class);
            List<RunResult> results = executor.execute();

            assertThat(results).singleElement().satisfies(result -> {
                assertThat(result.outcome()).isEqualTo(Outcome.EXECUTED);
                assertThat(result.taskId()).isEqualTo("test/warm-up");
            });
            assertThat(context.getBean(Capture.class).configs)
                    .singleElement()
                    .satisfies(config -> assertThat(config).containsEntry("token", "secret-token"));
            StartupTaskState state = context.getBean(StartupTaskState.class);
            assertThat(state.isCompleted()).isTrue();
            assertThat(state.isFailed()).isFalse();
        });
    }

    @Test
    void secondApplicationStartupSkipsTheSameVersion() {
        enabled("classpath:startup/warm-up.yaml").run(context ->
                assertThat(context.getBean(StartupTaskExecutor.class).execute())
                        .singleElement()
                        .satisfies(result -> assertThat(result.outcome()).isEqualTo(Outcome.EXECUTED)));

        enabled("classpath:startup/warm-up.yaml").run(context ->
                assertThat(context.getBean(StartupTaskExecutor.class).execute())
                        .singleElement()
                        .satisfies(result -> assertThat(result.outcome()).isEqualTo(Outcome.SKIPPED_ALREADY_DONE)));
    }

    @Test
    void warnPolicyKeepsTheApplicationAlive() {
        enabled("classpath:startup/failing.yaml")
                .withPropertyValues("startup-task.failure-policy=warn")
                .run(context -> {
                    List<RunResult> results = context.getBean(StartupTaskExecutor.class).execute();
                    assertThat(results).singleElement()
                            .satisfies(result -> assertThat(result.outcome()).isEqualTo(Outcome.FAILED));
                    StartupTaskState state = context.getBean(StartupTaskState.class);
                    assertThat(state.isCompleted()).isTrue();
                    assertThat(state.isFailed()).isTrue();
                });
    }

    @Test
    void failFastPolicyAbortsStartup() {
        enabled("classpath:startup/failing.yaml")
                .withPropertyValues("startup-task.failure-policy=fail-fast")
                .run(context -> assertThatThrownBy(() -> context.getBean(StartupTaskExecutor.class).execute())
                        .isInstanceOf(StartupTaskException.class)
                        .hasMessageContaining("test/failing"));
    }

    @Test
    void healthIsOutOfServiceUntilTasksComplete() {
        enabled("classpath:startup/warm-up.yaml").run(context -> {
            HealthIndicator health = context.getBean("startupTaskHealthIndicator", HealthIndicator.class);
            assertThat(health.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);

            context.getBean(StartupTaskExecutor.class).execute();
            Health after = health.health();
            assertThat(after.getStatus()).isEqualTo(Status.UP);
            assertThat(after.getDetails()).containsKey("tasks");
        });
    }

    @Test
    void runnerIsRegisteredWithLowestPrecedence() {
        enabled("classpath:startup/warm-up.yaml").run(context ->
                assertThat(context.getBean(StartupTaskApplicationRunner.class).getOrder())
                        .isEqualTo(Ordered.LOWEST_PRECEDENCE));
    }

    @Test
    void startsWithoutActuatorOnTheClasspath() {
        enabled("classpath:startup/warm-up.yaml")
                .withClassLoader(new FilteredClassLoader(HealthIndicator.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(StartupTaskExecutor.class);
                    assertThat(context).doesNotHaveBean("startupTaskHealthIndicator");
                    assertThat(context.getBean(StartupTaskExecutor.class).execute())
                            .singleElement()
                            .satisfies(result -> assertThat(result.outcome()).isEqualTo(Outcome.EXECUTED));
                });
    }

    @Test
    void duplicateTaskIdsAreRejected() {
        enabled("classpath:startup/warm-up.yaml,classpath:startup/warm-up.yaml")
                .run(context -> assertThatThrownBy(() -> context.getBean(StartupTaskExecutor.class).execute())
                        .isInstanceOf(StartupTaskException.class)
                        .hasMessageContaining("could not run"));
    }

    @Configuration(proxyBeanMethods = false)
    static class TestConfiguration {

        @Bean
        DataSource dataSource(@Value("${test.db}") String url) {
            return new DriverManagerDataSource(url);
        }

        @Bean
        Capture capture() {
            return new Capture();
        }

        @Bean
        TestHandler testHandler(Capture capture) {
            return new TestHandler(capture);
        }

        @Bean
        FailingHandler failingHandler() {
            return new FailingHandler();
        }
    }

    static class Capture {
        final List<Map<String, Object>> configs = new CopyOnWriteArrayList<>();
    }

    @StartupTaskHandler("test_handler")
    static class TestHandler implements TaskHandler {

        private final Capture capture;

        TestHandler(Capture capture) {
            this.capture = capture;
        }

        @Override
        public void handle(Map<String, Object> config) {
            capture.configs.add(Map.copyOf(config));
        }
    }

    @StartupTaskHandler("failing_handler")
    static class FailingHandler implements TaskHandler {

        @Override
        public void handle(Map<String, Object> config) {
            throw new IllegalStateException("boom");
        }
    }
}
