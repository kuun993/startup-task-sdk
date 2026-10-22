package io.startuptask.examples;

import io.startuptask.Coordinator;
import io.startuptask.Logs;
import io.startuptask.RunResult;
import io.startuptask.Schema;
import io.startuptask.SqliteStore;
import io.startuptask.TaskSpec;
import io.startuptask.TaskSpecLoader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.logging.Level;

public final class ExampleRunner {

    private static final String DEFAULT_CONFIG_CENTER_URL = "http://127.0.0.1:8080";
    private static final String DEFAULT_CONFIG_CENTER_TOKEN = "example-token";
    private static final String DEFAULT_APP_NAME = "demo-app";

    private ExampleRunner() {
    }

    public static void main(String[] args) throws Exception {
        Path examplesDir = Path.of(args.length > 0 ? args[0] : "examples");
        Path stateDir = Path.of(args.length > 1 ? args[1] : "target/examples");
        Files.createDirectories(stateDir);
        String jdbcUrl = "jdbc:sqlite:" + stateDir.resolve("startup-task.db").toAbsolutePath();

        String configCenterUrl = setting("CONFIG_CENTER_URL", DEFAULT_CONFIG_CENTER_URL);
        String configCenterToken = setting("CONFIG_CENTER_TOKEN", DEFAULT_CONFIG_CENTER_TOKEN);
        String appName = setting("APP_NAME", DEFAULT_APP_NAME);

        SqliteStore store = new SqliteStore(jdbcUrl);
        Schema.ensure(store);

        // sqlite-jdbc resets the JUL configuration when it opens its first connection,
        // so install the SDK log handler afterwards.
        Logs.configure(Level.INFO);

        Coordinator coordinator = new Coordinator(store, null);
        coordinator.registerHandler("warm_up_cache", config ->
                System.out.println("  warm_up_cache: keys=" + config.get("keys")
                        + " ttl_seconds=" + config.get("ttl_seconds")));

        TaskSpec cacheSpec = TaskSpecLoader.load(examplesDir.resolve("warm_up_cache.yaml"));
        TaskSpec settingsSpec = loadSettingsSpec(examplesDir, configCenterUrl, configCenterToken, appName);

        System.out.println("owner=" + coordinator.ownerId());
        System.out.println("sqlite=" + jdbcUrl);
        System.out.println("config-center=" + configCenterUrl + " app=" + appName);
        System.out.println("run #1");
        print(coordinator.runAll(List.of(cacheSpec, settingsSpec)));
        System.out.println("run #2 (same specs: success is skipped, failed is retried)");
        print(coordinator.runAll(List.of(cacheSpec, settingsSpec)));
    }

    private static void print(List<RunResult> results) {
        for (RunResult result : results) {
            System.out.println("  " + result);
        }
    }

    private static TaskSpec loadSettingsSpec(Path examplesDir, String url, String token, String appName)
            throws IOException {
        // Environment variables cannot be set from inside the process, so the example resolves the
        // three placeholders with the "-D -> environment variable -> default" precedence itself.
        // The baseDir is preserved so that body_path (app_settings.json) resolves against examples/.
        String yaml = Files.readString(examplesDir.resolve("publish_app_settings.yaml"), StandardCharsets.UTF_8)
                .replace("${CONFIG_CENTER_URL}", url)
                .replace("${CONFIG_CENTER_TOKEN}", token)
                .replace("${APP_NAME}", appName);
        TaskSpec parsed = TaskSpecLoader.loadText(yaml);
        return new TaskSpec(parsed.taskId(), parsed.version(), parsed.description(), parsed.timeoutSeconds(),
                parsed.executor(), parsed.handler(), parsed.config(), examplesDir.toAbsolutePath());
    }

    private static String setting(String name, String fallback) {
        String property = System.getProperty(name);
        if (property != null && !property.isBlank()) {
            return property;
        }
        String environment = System.getenv(name);
        if (environment != null && !environment.isBlank()) {
            return environment;
        }
        return fallback;
    }
}
