package io.startuptask;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExamplesTest {

    private static final Path EXAMPLES = Path.of("examples").toAbsolutePath();
    private static final String CONFIG_CENTER_URL = "CONFIG_CENTER_URL";

    @TempDir
    Path tempDir;

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

    @Test
    void publishAppSettingsExampleParses() throws Exception {
        TaskSpec spec = TaskSpecLoader.load(EXAMPLES.resolve("publish_app_settings.yaml"));

        assertEquals("config/publish-app-settings", spec.taskId());
        assertEquals("2026-10-22-v1", spec.version());
        assertEquals("Publish application default settings to the config center", spec.description());
        assertEquals(120, spec.timeoutSeconds());
        assertEquals(TaskSpec.Executor.HTTP, spec.executor());
        assertEquals(EXAMPLES, spec.baseDir());

        assertEquals("PUT", spec.config().get("method"));
        assertEquals("${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings", spec.config().get("url"));

        @SuppressWarnings("unchecked")
        Map<String, Object> headers = (Map<String, Object>) spec.config().get("headers");
        assertEquals("Bearer ${CONFIG_CENTER_TOKEN}", headers.get("Authorization"));
        assertEquals("application/json", headers.get("Content-Type"));

        assertEquals("file", spec.config().get("body_source"));
        assertEquals("app_settings.json", spec.config().get("body_path"));

        @SuppressWarnings("unchecked")
        Map<String, Object> successCheck = (Map<String, Object>) spec.config().get("success_check");
        assertEquals(List.of(200, 201), TaskSpecLoader.parseStatuses(successCheck.get("http_status")));

        Path body = spec.baseDir().resolve((String) spec.config().get("body_path"));
        assertTrue(Files.isRegularFile(body), "body file must exist: " + body);
        String json = Files.readString(body);
        assertTrue(json.contains("\"feature_flags\""));
        assertTrue(json.contains("\"rate_limit_per_minute\""));
        assertTrue(json.contains("\"updated_by\": \"startup-task-sdk\""));
    }

    @Test
    void warmUpCacheExampleParsesAndRuns() {
        TaskSpec spec = TaskSpecLoader.load(EXAMPLES.resolve("warm_up_cache.yaml"));

        assertEquals("cache/warm-up", spec.taskId());
        assertEquals("2026-10-22-v1", spec.version());
        assertEquals("Warm up hot data in the cache", spec.description());
        assertEquals(60, spec.timeoutSeconds());
        assertEquals(TaskSpec.Executor.CUSTOM, spec.executor());
        assertEquals("warm_up_cache", spec.handler());
        assertEquals(List.of("home:feed", "home:recommend", "config:feature-flags"), spec.config().get("keys"));
        assertEquals(300, spec.config().get("ttl_seconds"));

        AtomicReference<Map<String, Object>> received = new AtomicReference<>();
        Coordinator coordinator = new Coordinator(db.store(), "local-example");
        coordinator.registerHandler("warm_up_cache", received::set);

        RunResult first = coordinator.run(spec);
        assertEquals(Outcome.EXECUTED, first.outcome());
        assertEquals(300, received.get().get("ttl_seconds"));
        assertEquals(List.of("home:feed", "home:recommend", "config:feature-flags"), received.get().get("keys"));

        RunResult second = coordinator.run(spec);
        assertEquals(Outcome.SKIPPED_ALREADY_DONE, second.outcome());
        assertTrue(db.locks().isEmpty());
    }

    @Test
    void exampleRunnerExecutesBothExamples() throws Exception {
        Path stateDir = tempDir.resolve("state");
        int closedPort = closedPort();
        String previous = System.getProperty(CONFIG_CENTER_URL);
        System.setProperty(CONFIG_CENTER_URL, "http://127.0.0.1:" + closedPort);
        try {
            io.startuptask.examples.ExampleRunner.main(new String[] {EXAMPLES.toString(), stateDir.toString()});
        } finally {
            if (previous == null) {
                System.clearProperty(CONFIG_CENTER_URL);
            } else {
                System.setProperty(CONFIG_CENTER_URL, previous);
            }
            Logger.getLogger("io.startuptask").setLevel(Level.OFF);
        }

        Path database = stateDir.resolve("startup-task.db");
        assertTrue(Files.isRegularFile(database), "example runner must create its sqlite database");

        Map<String, String> statuses = new HashMap<>();
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database.toAbsolutePath());
             PreparedStatement statement =
                     connection.prepareStatement("SELECT task_id, status FROM task_version_record");
             ResultSet rows = statement.executeQuery()) {
            while (rows.next()) {
                statuses.put(rows.getString(1), rows.getString(2));
            }
        }

        assertEquals("success", statuses.get("cache/warm-up"), "custom example must succeed");
        assertEquals("failed", statuses.get("config/publish-app-settings"),
                "unreachable config center must end as FAILED, not as an exception");
        assertEquals(2, statuses.size());
    }

    private static int closedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
