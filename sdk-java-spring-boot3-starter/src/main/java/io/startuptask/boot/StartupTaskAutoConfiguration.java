package io.startuptask.boot;

import io.startuptask.Coordinator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.sql.DataSource;

/**
 * Auto-configuration for the startup task SDK.
 *
 * <p>Activated by {@code startup-task.enabled=true}. Requires a single {@link DataSource}; the
 * dialect (PostgreSQL, MySQL/MariaDB, SQLite) is detected from the connection metadata.
 */
@AutoConfiguration(
        after = DataSourceAutoConfiguration.class,
        afterName = {
                "org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration",
                "org.springframework.boot.autoconfigure.sql.init.SqlInitializationAutoConfiguration"
        })
@ConditionalOnClass({Coordinator.class, DataSource.class})
@ConditionalOnProperty(prefix = "startup-task", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(StartupTaskProperties.class)
public class StartupTaskAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public StartupTaskState startupTaskState() {
        return new StartupTaskState();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnSingleCandidate(DataSource.class)
    public StartupTaskExecutor startupTaskExecutor(DataSource dataSource,
                                                   StartupTaskProperties properties,
                                                   ApplicationContext applicationContext,
                                                   Environment environment,
                                                   StartupTaskState state) {
        return new StartupTaskExecutor(dataSource, properties, applicationContext, environment, state);
    }

    @Bean
    @ConditionalOnMissingBean
    public StartupTaskApplicationRunner startupTaskApplicationRunner(StartupTaskExecutor executor) {
        return new StartupTaskApplicationRunner(executor);
    }

    /** Isolated so the starter still works when spring-boot-actuator is absent. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
    static class HealthConfiguration {

        @Bean
        @ConditionalOnMissingBean(name = "startupTaskHealthIndicator")
        public org.springframework.boot.actuate.health.HealthIndicator startupTaskHealthIndicator(
                StartupTaskState state) {
            return new StartupTaskHealthIndicator(state);
        }
    }
}
