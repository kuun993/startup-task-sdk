package io.startuptask;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class HttpExecutor {

    private static final Pattern ENV_PATTERN = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)}");

    private final HttpClient client;

    public HttpExecutor() {
        this(HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build());
    }

    public HttpExecutor(HttpClient client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    public void execute(TaskSpec spec) {
        Map<String, Object> config = spec.config();
        String method = requireText(config, "method").toUpperCase(Locale.ROOT);
        String url = substitute(requireText(config, "url"));
        List<Integer> expectedStatuses = successStatuses(config);
        Map<String, String> headers = headers(config);
        HttpRequest.BodyPublisher publisher = bodyPublisher(spec, config);

        HttpResponse<String> response;
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(spec.timeoutSeconds()));
            headers.forEach(builder::header);
            HttpRequest request = builder.method(method, publisher).build();
            response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TaskExecutionException("http request failed: interrupted", e);
        } catch (Exception e) {
            throw new TaskExecutionException("http request failed: " + Values.describe(e), e);
        }

        int status = response.statusCode();
        if (!expectedStatuses.contains(status)) {
            throw new TaskExecutionException("http status " + status + " not in "
                    + formatStatuses(expectedStatuses) + " (body: " + response.body() + ")");
        }
    }

    static String substitute(String value) {
        if (value == null || value.indexOf("${") < 0) {
            return value;
        }
        Matcher matcher = ENV_PATTERN.matcher(value);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            String name = matcher.group(1);
            String replacement = System.getenv(name);
            if (replacement == null) {
                throw new TaskExecutionException("environment variable \"" + name + "\" is not set");
            }
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private static Map<String, String> headers(Map<String, Object> config) {
        Map<String, String> headers = new LinkedHashMap<>();
        if (config.get("headers") instanceof Map<?, ?> raw) {
            for (Map.Entry<?, ?> entry : raw.entrySet()) {
                headers.put(String.valueOf(entry.getKey()), substitute(String.valueOf(entry.getValue())));
            }
        }
        return headers;
    }

    private static HttpRequest.BodyPublisher bodyPublisher(TaskSpec spec, Map<String, Object> config) {
        String bodySource = config.get("body_source") instanceof String source ? source : "none";
        return switch (bodySource) {
            // The body file is an opaque payload supplied by the caller, so it is forwarded byte for
            // byte: reading it with readString would corrupt any non-UTF-8 content.
            case "file" -> HttpRequest.BodyPublishers.ofByteArray(
                    readBodyFile(spec, requireText(config, "body_path")));
            case "inline" -> HttpRequest.BodyPublishers.ofString(
                    substitute(config.get("body") instanceof String body ? body : ""), StandardCharsets.UTF_8);
            default -> HttpRequest.BodyPublishers.noBody();
        };
    }

    private static byte[] readBodyFile(TaskSpec spec, String bodyPath) {
        Path path = Path.of(bodyPath);
        if (!path.isAbsolute()) {
            path = spec.baseDir() == null ? path.toAbsolutePath() : spec.baseDir().resolve(bodyPath);
        }
        try {
            return Files.readAllBytes(path);
        } catch (IOException e) {
            throw new TaskExecutionException("body file \"" + path + "\" cannot be read: " + Values.describe(e), e);
        }
    }

    private static List<Integer> successStatuses(Map<String, Object> config) {
        if (config.get("success_check") instanceof Map<?, ?> successCheck) {
            Object rawStatus = successCheck.get("http_status");
            if (rawStatus != null) {
                return TaskSpecLoader.parseStatuses(rawStatus);
            }
        }
        List<Integer> defaults = new ArrayList<>();
        defaults.add(200);
        return defaults;
    }

    private static String formatStatuses(List<Integer> statuses) {
        return statuses.stream().map(String::valueOf).collect(Collectors.joining(", ", "[", "]"));
    }

    private static String requireText(Map<String, Object> config, String key) {
        Object value = config.get(key);
        if (!(value instanceof String text) || text.isBlank()) {
            throw new TaskExecutionException("execution.config." + key + " is required");
        }
        return text;
    }
}
