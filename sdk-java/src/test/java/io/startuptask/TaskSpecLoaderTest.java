package io.startuptask;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskSpecLoaderTest {

    private static final String HTTP_SPEC = """
            task_id: config/publish-app-settings
            version: "2026-10-22-v1"
            description: Publish application default settings to the config center
            timeout_seconds: 60
            execution:
              executor: http
              config:
                method: PUT
                url: "${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings"
                headers:
                  Authorization: "Bearer ${CONFIG_CENTER_TOKEN}"
                body_source: inline
                body: '{"a":1}'
                success_check:
                  http_status: [200, 201]
            """;

    @TempDir
    Path tempDir;

    @Test
    void parsesHttpSpec() {
        TaskSpec spec = TaskSpecLoader.loadText(HTTP_SPEC);

        assertEquals("config/publish-app-settings", spec.taskId());
        assertEquals("2026-10-22-v1", spec.version());
        assertEquals("Publish application default settings to the config center", spec.description());
        assertEquals(60, spec.timeoutSeconds());
        assertEquals(TaskSpec.Executor.HTTP, spec.executor());
        assertNull(spec.handler());
        assertEquals("PUT", spec.config().get("method"));
        assertEquals("inline", spec.config().get("body_source"));
        assertEquals(List.of(200, 201),
                TaskSpecLoader.parseStatuses(((Map<?, ?>) spec.config().get("success_check")).get("http_status")));
        assertNull(spec.baseDir());
    }

    @Test
    void appliesDefaults() {
        TaskSpec spec = TaskSpecLoader.loadText("""
                task_id: minimal-task
                version: "1"
                execution:
                  executor: http
                  config:
                    method: GET
                    url: https://example.com/
                """);

        assertEquals(TaskSpec.DEFAULT_TIMEOUT_SECONDS, spec.timeoutSeconds());
        assertNull(spec.description());
        assertEquals("none", spec.config().getOrDefault("body_source", "none"));
        assertNull(spec.config().get("headers"));
    }

    @Test
    void allowsEmptyDescriptionAndInlineBody() {
        TaskSpec spec = TaskSpecLoader.loadText("""
                task_id: empty-strings
                version: "1"
                description: ""
                execution:
                  executor: http
                  config:
                    method: POST
                    url: https://example.com/
                    body_source: inline
                    body: ""
                """);

        assertEquals("", spec.description());
        assertEquals("", spec.config().get("body"));
    }

    @Test
    void rejectsEmptyBodyPathForFileBody() {
        assertMessage("execution.config.body_path is required", """
                task_id: x
                version: "1"
                execution:
                  executor: http
                  config:
                    method: POST
                    url: https://example.com/
                    body_source: file
                    body_path: ""
                """);
    }

    @Test
    void parsesCustomSpecWithDeepCopiedConfig() {
        TaskSpec spec = TaskSpecLoader.loadText("""
                task_id: cache/warm-up
                version: "1"
                execution:
                  executor: custom
                  handler: warm_up_cache
                  config:
                    keys: ["home:feed"]
                    ttl_seconds: 300
                    nested:
                      flags: [a, b]
                """);

        assertEquals(TaskSpec.Executor.CUSTOM, spec.executor());
        assertEquals("warm_up_cache", spec.handler());
        assertEquals(300, spec.config().get("ttl_seconds"));

        @SuppressWarnings("unchecked")
        Map<String, Object> handed = spec.handlerConfig();
        handed.put("ttl_seconds", 999);
        assertEquals(300, spec.config().get("ttl_seconds"), "handler config must be a deep copy");
    }

    @Test
    void customSpecWithoutConfigGetsEmptyMap() {
        TaskSpec spec = TaskSpecLoader.loadText("""
                task_id: no-config
                version: "1"
                execution:
                  executor: custom
                  handler: noop
                """);

        assertTrue(spec.config().isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> spec.config().put("x", 1));
    }

    @Test
    void loadsFromFilePathAndSetsBaseDir() throws Exception {
        Path file = tempDir.resolve("publish_app_settings.yaml");
        Files.writeString(file, HTTP_SPEC);

        TaskSpec spec = TaskSpecLoader.load(file.toString());

        assertEquals("config/publish-app-settings", spec.taskId());
        assertEquals(tempDir.toAbsolutePath(), spec.baseDir());
    }

    @Test
    void missingFileIsReported() {
        SpecException error = assertThrows(SpecException.class,
                () -> TaskSpecLoader.load(tempDir.resolve("nope.yaml").toString()));
        assertTrue(error.getMessage().contains("does not exist"), error.getMessage());
    }

    @Test
    void rejectsMissingTaskId() {
        assertMessage("task.task_id is required", """
                version: "1"
                execution:
                  executor: custom
                  handler: h
                """);
    }

    @Test
    void rejectsBlankTaskId() {
        assertMessage("task.task_id must not be empty", """
                task_id: "  "
                version: "1"
                execution:
                  executor: custom
                  handler: h
                """);
    }

    @Test
    void rejectsInvalidTaskIdCharacters() {
        assertMessage("task.task_id must match", """
                task_id: "bad task!"
                version: "1"
                execution:
                  executor: custom
                  handler: h
                """);
    }

    @Test
    void rejectsTooLongTaskId() {
        String taskId = "a".repeat(129);
        assertMessage("task.task_id must be 1..128 characters", """
                task_id: "%s"
                version: "1"
                execution:
                  executor: custom
                  handler: h
                """.formatted(taskId));
    }

    @Test
    void rejectsMissingVersion() {
        assertMessage("task.version is required", """
                task_id: x
                execution:
                  executor: custom
                  handler: h
                """);
    }

    @Test
    void rejectsNonPositiveTimeout() {
        assertMessage("task.timeout_seconds must be > 0", """
                task_id: x
                version: "1"
                timeout_seconds: 0
                execution:
                  executor: custom
                  handler: h
                """);
    }

    @Test
    void rejectsNonIntegerTimeout() {
        assertMessage("task.timeout_seconds must be an integer", """
                task_id: x
                version: "1"
                timeout_seconds: "60"
                execution:
                  executor: custom
                  handler: h
                """);
    }

    @Test
    void rejectsMissingExecution() {
        assertMessage("task.execution is required", """
                task_id: x
                version: "1"
                """);
    }

    @Test
    void rejectsMissingExecutor() {
        assertMessage("execution.executor is required", """
                task_id: x
                version: "1"
                execution:
                  config: {}
                """);
    }

    @Test
    void rejectsUnknownExecutor() {
        assertMessage("execution.executor must be one of [http, custom]", """
                task_id: x
                version: "1"
                execution:
                  executor: Http
                """);
    }

    @Test
    void rejectsUnknownTopLevelKey() {
        assertMessage("task.extra is not allowed", """
                task_id: x
                version: "1"
                extra: true
                execution:
                  executor: custom
                  handler: h
                """);
    }

    @Test
    void rejectsHttpWithoutConfig() {
        assertMessage("execution.config is required", """
                task_id: x
                version: "1"
                execution:
                  executor: http
                """);
    }

    @Test
    void rejectsHttpWithoutMethod() {
        assertMessage("execution.config.method is required", """
                task_id: x
                version: "1"
                execution:
                  executor: http
                  config:
                    url: https://example.com/
                """);
    }

    @Test
    void rejectsHttpWithoutUrl() {
        assertMessage("execution.config.url is required", """
                task_id: x
                version: "1"
                execution:
                  executor: http
                  config:
                    method: GET
                """);
    }

    @Test
    void rejectsBodyFileWithoutPath() {
        assertMessage("execution.config.body_path is required", """
                task_id: x
                version: "1"
                execution:
                  executor: http
                  config:
                    method: POST
                    url: https://example.com/
                    body_source: file
                """);
    }

    @Test
    void rejectsUnknownBodySource() {
        assertMessage("execution.config.body_source must be one of [file, inline, none]", """
                task_id: x
                version: "1"
                execution:
                  executor: http
                  config:
                    method: POST
                    url: https://example.com/
                    body_source: stdin
                """);
    }

    @Test
    void rejectsUnknownHttpConfigKey() {
        assertMessage("execution.config.verb is not allowed", """
                task_id: x
                version: "1"
                execution:
                  executor: http
                  config:
                    method: GET
                    url: https://example.com/
                    verb: GET
                """);
    }

    @Test
    void rejectsNonStringHeaderValue() {
        assertMessage("execution.config.headers.X-Retry must be a string", """
                task_id: x
                version: "1"
                execution:
                  executor: http
                  config:
                    method: GET
                    url: https://example.com/
                    headers:
                      X-Retry: 3
                """);
    }

    @Test
    void rejectsInvalidHttpStatusShape() {
        assertMessage("execution.config.success_check.http_status", """
                task_id: x
                version: "1"
                execution:
                  executor: http
                  config:
                    method: GET
                    url: https://example.com/
                    success_check:
                      http_status: ok
                """);
    }

    @Test
    void rejectsCustomWithoutHandler() {
        assertMessage("execution.handler is required", """
                task_id: x
                version: "1"
                execution:
                  executor: custom
                """);
    }

    @Test
    void rejectsInvalidYaml() {
        assertMessage("task: invalid yaml", """
                task_id: x
                version: [unclosed
                """);
    }

    private static void assertMessage(String expectedFragment, String yaml) {
        SpecException error = assertThrows(SpecException.class, () -> TaskSpecLoader.loadText(yaml));
        assertTrue(error.getMessage().contains(expectedFragment),
                "expected \"" + expectedFragment + "\" in: " + error.getMessage());
    }
}
