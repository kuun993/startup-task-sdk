package io.startuptask;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class HttpExecutorTest {

    private static final Set<String> RESERVED_HEADERS = Set.of(
            "host", "connection", "content-length", "date", "expect", "from", "upgrade", "via", "warning");

    @TempDir
    Path tempDir;

    private final HttpExecutor executor = new HttpExecutor();
    private HttpServer server;
    private ExecutorService serverPool;
    private String baseUrl;
    private final AtomicReference<String> lastMethod = new AtomicReference<>();
    private final AtomicReference<String> lastBody = new AtomicReference<>();
    private final AtomicReference<byte[]> lastRawBody = new AtomicReference<>();
    private final AtomicReference<Map<String, List<String>>> lastHeaders = new AtomicReference<>();

    @BeforeAll
    static void quietLogs() {
        Logger.getLogger("io.startuptask").setLevel(Level.OFF);
    }

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        serverPool = Executors.newFixedThreadPool(4);
        server.setExecutor(serverPool);
        server.createContext("/ok", exchange -> respond(exchange, 200, "{\"status\":\"ok\"}"));
        server.createContext("/created", exchange -> respond(exchange, 201, "{\"status\":\"created\"}"));
        server.createContext("/fail", exchange -> respond(exchange, 500, "boom"));
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
        if (serverPool != null) {
            serverPool.shutdownNow();
        }
    }

    @Test
    void postsInlineBodyWithHeadersAndEnvSubstitution() {
        String envName = safeEnvVarName();
        assumeTrue(envName != null, "no environment variable usable for substitution");
        String envValue = System.getenv(envName);

        TaskSpec spec = TaskSpecLoader.loadText("""
                task_id: http-inline
                version: "1"
                timeout_seconds: 10
                execution:
                  executor: http
                  config:
                    method: post
                    url: "%s/ok"
                    headers:
                      X-Env: "value=${%s}"
                      Content-Type: application/json
                    body_source: inline
                    body: '{"token":"${%s}"}'
                """.formatted(baseUrl, envName, envName));

        executor.execute(spec);

        assertEquals("POST", lastMethod.get());
        assertEquals("value=" + envValue, firstHeader("X-Env"));
        assertEquals("{\"token\":\"" + envValue + "\"}", lastBody.get());
    }

    @Test
    void fileBodyIsResolvedRelativeToSpecDirectory() throws IOException {
        Path payload = tempDir.resolve("payload.json");
        Files.writeString(payload, "{\"from\":\"file\",\"raw\":\"${NOT_SUBSTITUTED}\"}");
        Path specFile = tempDir.resolve("file_body.yaml");
        Files.writeString(specFile, """
                task_id: http-file-body
                version: "1"
                execution:
                  executor: http
                  config:
                    method: PUT
                    url: "%s/ok"
                    body_source: file
                    body_path: payload.json
                """.formatted(baseUrl));

        executor.execute(TaskSpecLoader.load(specFile));

        assertEquals("PUT", lastMethod.get());
        assertEquals("{\"from\":\"file\",\"raw\":\"${NOT_SUBSTITUTED}\"}", lastBody.get(),
                "file bodies are passed through without environment substitution");
    }

    @Test
    void fileBodyPreservesNonUtf8Bytes() throws IOException {
        byte[] payload = new byte[] {0x00, (byte) 0xFF, 0x7B, 0x22, (byte) 0xC3, 0x28, 0x00};
        Files.write(tempDir.resolve("binary.bin"), payload);
        Path specFile = tempDir.resolve("binary_body.yaml");
        Files.writeString(specFile, """
                task_id: http-binary-body
                version: "1"
                execution:
                  executor: http
                  config:
                    method: POST
                    url: "%s/ok"
                    body_source: file
                    body_path: binary.bin
                """.formatted(baseUrl));

        executor.execute(TaskSpecLoader.load(specFile));

        assertArrayEquals(payload, lastRawBody.get(),
                "body files must be sent byte-for-byte even when they are not valid UTF-8");
    }

    @Test
    void nonExpectedStatusIsReportedWithFixedMessage() {
        TaskSpec spec = TaskSpecLoader.loadText("""
                task_id: http-fail
                version: "1"
                execution:
                  executor: http
                  config:
                    method: GET
                    url: "%s/fail"
                """.formatted(baseUrl));

        TaskExecutionException error = assertThrows(TaskExecutionException.class, () -> executor.execute(spec));
        assertEquals("http status 500 not in [200] (body: boom)", error.getMessage());
    }

    @Test
    void successCheckListAcceptsConfiguredStatuses() {
        TaskSpec spec = TaskSpecLoader.loadText("""
                task_id: http-created
                version: "1"
                execution:
                  executor: http
                  config:
                    method: POST
                    url: "%s/created"
                    success_check:
                      http_status: [200, 201]
                """.formatted(baseUrl));

        executor.execute(spec);

        assertEquals("POST", lastMethod.get());
    }

    @Test
    void missingEnvironmentVariableIsReported() {
        TaskSpec spec = TaskSpecLoader.loadText("""
                task_id: http-missing-env
                version: "1"
                execution:
                  executor: http
                  config:
                    method: GET
                    url: "%s/${STARTUP_TASK_SDK_MISSING_ENV}"
                """.formatted(baseUrl));

        TaskExecutionException error = assertThrows(TaskExecutionException.class, () -> executor.execute(spec));
        assertEquals("environment variable \"STARTUP_TASK_SDK_MISSING_ENV\" is not set", error.getMessage());
    }

    @Test
    void transportErrorIsWrapped() throws IOException {
        int freePort;
        try (ServerSocket socket = new ServerSocket(0)) {
            freePort = socket.getLocalPort();
        }
        TaskSpec spec = TaskSpecLoader.loadText("""
                task_id: http-unreachable
                version: "1"
                execution:
                  executor: http
                  config:
                    method: GET
                    url: "http://127.0.0.1:%d/nowhere"
                """.formatted(freePort));

        TaskExecutionException error = assertThrows(TaskExecutionException.class, () -> executor.execute(spec));
        assertTrue(error.getMessage().startsWith("http request failed:"), error.getMessage());
    }

    @Test
    void coordinatorTurnsHttpFailureIntoFailedOutcome() throws Exception {
        SqliteFixture db = new SqliteFixture();
        try {
            TaskSpec spec = TaskSpecLoader.loadText("""
                    task_id: http-coordinated
                    version: "1"
                    execution:
                      executor: http
                      config:
                        method: GET
                        url: "%s/fail"
                    """.formatted(baseUrl));
            Coordinator coordinator = new Coordinator(db.store(), "pod-a");

            RunResult result = coordinator.run(spec);

            assertEquals(Outcome.FAILED, result.outcome());
            assertEquals("http status 500 not in [200] (body: boom)", result.error());
            assertTrue(db.locks().isEmpty());
            assertEquals("failed", db.versions().get(0).status());
        } finally {
            db.close();
        }
    }

    private void respond(HttpExchange exchange, int status, String body) throws IOException {
        lastMethod.set(exchange.getRequestMethod());
        lastHeaders.set(Map.copyOf(exchange.getRequestHeaders()));
        byte[] requestBody = exchange.getRequestBody().readAllBytes();
        lastRawBody.set(requestBody);
        lastBody.set(new String(requestBody, StandardCharsets.UTF_8));
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, payload.length);
        exchange.getResponseBody().write(payload);
        exchange.close();
    }

    private String firstHeader(String name) {
        Map<String, List<String>> headers = lastHeaders.get();
        assertNotNull(headers, "no request captured");
        return headers.entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(name))
                .map(entry -> entry.getValue().get(0))
                .findFirst()
                .orElse(null);
    }

    private static String safeEnvVarName() {
        return System.getenv().entrySet().stream()
                .filter(entry -> entry.getKey().matches("[A-Za-z_][A-Za-z0-9_]*"))
                .filter(entry -> !RESERVED_HEADERS.contains(entry.getKey().toLowerCase()))
                .filter(entry -> entry.getValue().matches("[\\x20-\\x7E]*"))
                .filter(entry -> !entry.getValue().isEmpty())
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }
}
