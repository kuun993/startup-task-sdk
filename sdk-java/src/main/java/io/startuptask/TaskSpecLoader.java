package io.startuptask;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

public final class TaskSpecLoader {

    private static final Pattern TASK_ID_PATTERN = Pattern.compile("^[A-Za-z0-9._:/-]+$");

    private static final Set<String> TASK_KEYS =
            Set.of("task_id", "version", "description", "timeout_seconds", "execution");
    private static final Set<String> EXECUTION_KEYS = Set.of("executor", "handler", "config");
    private static final Set<String> HTTP_KEYS = Set.of(
            "method", "url", "headers", "body_source", "body_path", "body", "success_check");
    private static final Set<String> SUCCESS_CHECK_KEYS = Set.of("http_status");
    private static final Set<String> BODY_SOURCES = Set.of("file", "inline", "none");

    private TaskSpecLoader() {
    }

    public static TaskSpec load(String yamlTextOrPath) {
        if (yamlTextOrPath == null) {
            throw new SpecException("task: yaml text or path must not be null");
        }
        Path file = existingFile(yamlTextOrPath);
        if (file != null) {
            return load(file);
        }
        if (looksLikeSpecPath(yamlTextOrPath)) {
            throw new SpecException("task: spec file \"" + yamlTextOrPath.strip() + "\" does not exist");
        }
        return parse(yamlTextOrPath, null);
    }

    public static TaskSpec load(Path file) {
        Objects.requireNonNull(file, "file");
        if (!Files.isRegularFile(file)) {
            throw new SpecException("task: spec file \"" + file + "\" does not exist");
        }
        String text;
        try {
            text = Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new SpecException("task: cannot read spec file \"" + file + "\": " + e.getMessage(), e);
        }
        return parse(text, file.toAbsolutePath().getParent());
    }

    public static TaskSpec loadText(String yamlText) {
        if (yamlText == null) {
            throw new SpecException("task: yaml text must not be null");
        }
        return parse(yamlText, null);
    }

    private static TaskSpec parse(String text, Path baseDir) {
        Object root = parseYaml(text);
        if (root == null) {
            throw new SpecException("task: spec must not be empty");
        }
        Map<String, Object> task = asMap(root, "task", "spec must be a mapping");
        rejectUnknown(task, TASK_KEYS, "task");

        String taskId = requireString(task, "task_id", "task");
        requireRange(taskId, "task.task_id", 128);
        if (!TASK_ID_PATTERN.matcher(taskId).matches()) {
            throw new SpecException("task.task_id must match " + TASK_ID_PATTERN.pattern()
                    + " (got \"" + taskId + "\")");
        }

        String version = requireString(task, "version", "task");
        requireRange(version, "task.version", 64);

        String description = optionalText(task, "description", "task");

        int timeoutSeconds = TaskSpec.DEFAULT_TIMEOUT_SECONDS;
        Object rawTimeout = task.get("timeout_seconds");
        if (rawTimeout != null) {
            Long parsed = asLong(rawTimeout);
            if (parsed == null || parsed < Integer.MIN_VALUE || parsed > Integer.MAX_VALUE) {
                throw new SpecException("task.timeout_seconds must be an integer");
            }
            if (parsed <= 0) {
                throw new SpecException("task.timeout_seconds must be > 0");
            }
            timeoutSeconds = parsed.intValue();
        }

        Object rawExecution = task.get("execution");
        if (rawExecution == null) {
            throw new SpecException("task.execution is required");
        }
        Map<String, Object> execution = asMap(rawExecution, "execution", "execution must be an object");
        rejectUnknown(execution, EXECUTION_KEYS, "execution");

        String executorName = requireString(execution, "executor", "execution");
        TaskSpec.Executor executor = switch (executorName) {
            case "http" -> TaskSpec.Executor.HTTP;
            case "custom" -> TaskSpec.Executor.CUSTOM;
            default -> throw new SpecException(
                    "execution.executor must be one of [http, custom] (got \"" + executorName + "\")");
        };

        String handler = optionalString(execution, "handler", "execution");
        Map<String, Object> config;
        if (executor == TaskSpec.Executor.HTTP) {
            handler = null;
            config = parseHttpConfig(execution);
        } else {
            if (handler == null) {
                throw new SpecException("execution.handler is required");
            }
            Object rawConfig = execution.get("config");
            config = rawConfig == null ? Map.of() : asMap(rawConfig, "execution.config", "execution.config must be an object");
        }

        return new TaskSpec(taskId, version, description, timeoutSeconds, executor, handler, config, baseDir);
    }

    private static Map<String, Object> parseHttpConfig(Map<String, Object> execution) {
        Object rawConfig = execution.get("config");
        if (rawConfig == null) {
            throw new SpecException("execution.config is required");
        }
        Map<String, Object> config = asMap(rawConfig, "execution.config", "execution.config must be an object");
        rejectUnknown(config, HTTP_KEYS, "execution.config");

        requireString(config, "method", "execution.config");
        requireString(config, "url", "execution.config");

        Object rawHeaders = config.get("headers");
        if (rawHeaders != null) {
            Map<String, Object> headers = asMap(rawHeaders, "execution.config.headers",
                    "execution.config.headers must be an object");
            for (Map.Entry<String, Object> entry : headers.entrySet()) {
                if (!(entry.getValue() instanceof String)) {
                    throw new SpecException("execution.config.headers." + entry.getKey() + " must be a string");
                }
            }
        }

        String bodySource = optionalString(config, "body_source", "execution.config");
        if (bodySource == null) {
            bodySource = "none";
        } else if (!BODY_SOURCES.contains(bodySource)) {
            throw new SpecException("execution.config.body_source must be one of [file, inline, none] (got \""
                    + bodySource + "\")");
        }

        if ("file".equals(bodySource)) {
            requireTextKey(config, "body_path", "execution.config");
        } else {
            optionalText(config, "body_path", "execution.config");
        }

        optionalText(config, "body", "execution.config");

        Object rawSuccessCheck = config.get("success_check");
        if (rawSuccessCheck != null) {
            Map<String, Object> successCheck = asMap(rawSuccessCheck, "execution.config.success_check",
                    "execution.config.success_check must be an object");
            rejectUnknown(successCheck, SUCCESS_CHECK_KEYS, "execution.config.success_check");
            Object rawStatus = successCheck.get("http_status");
            if (rawStatus != null) {
                parseStatuses(rawStatus);
            }
        }

        return Values.deepCopyMap(config);
    }

    static List<Integer> parseStatuses(Object rawStatus) {
        List<Integer> statuses = new ArrayList<>();
        if (rawStatus instanceof Number number) {
            statuses.add(asIntStatus(number));
            return statuses;
        }
        if (rawStatus instanceof List<?> list) {
            if (list.isEmpty()) {
                throw new SpecException("execution.config.success_check.http_status must not be an empty list");
            }
            for (Object item : list) {
                if (!(item instanceof Number number)) {
                    throw new SpecException(
                            "execution.config.success_check.http_status must be an integer or a list of integers");
                }
                statuses.add(asIntStatus(number));
            }
            return statuses;
        }
        throw new SpecException(
                "execution.config.success_check.http_status must be an integer or a list of integers");
    }

    private static int asIntStatus(Number number) {
        long value = number.longValue();
        if (number.doubleValue() != (double) value || value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new SpecException(
                    "execution.config.success_check.http_status must be an integer or a list of integers");
        }
        return (int) value;
    }

    private static Object parseYaml(String text) {
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        try {
            return new Yaml(new SafeConstructor(options)).load(text);
        } catch (YAMLException e) {
            throw new SpecException("task: invalid yaml: " + e.getMessage(), e);
        }
    }

    private static Path existingFile(String text) {
        if (text.indexOf('\n') >= 0 || text.indexOf('\r') >= 0) {
            return null;
        }
        String trimmed = text.strip();
        if (trimmed.isEmpty()) {
            return null;
        }
        try {
            Path path = Path.of(trimmed);
            return Files.isRegularFile(path) ? path : null;
        } catch (InvalidPathException e) {
            return null;
        }
    }

    private static boolean looksLikeSpecPath(String text) {
        String trimmed = text.strip();
        if (trimmed.isEmpty() || trimmed.indexOf('\n') >= 0 || trimmed.indexOf('\r') >= 0) {
            return false;
        }
        String lower = trimmed.toLowerCase(Locale.ROOT);
        return lower.endsWith(".yaml") || lower.endsWith(".yml");
    }

    private static Map<String, Object> asMap(Object value, String path, String message) {
        if (!(value instanceof Map<?, ?> map)) {
            throw new SpecException(message);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            result.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return result;
    }

    private static void rejectUnknown(Map<String, Object> map, Set<String> allowed, String path) {
        for (String key : map.keySet()) {
            if (!allowed.contains(key)) {
                throw new SpecException(path + "." + key + " is not allowed");
            }
        }
    }

    private static String requireString(Map<String, Object> map, String key, String path) {
        Object value = map.get(key);
        if (value == null) {
            throw new SpecException(path + "." + key + " is required");
        }
        return asNonEmptyString(value, path + "." + key);
    }

    private static String optionalString(Map<String, Object> map, String key, String path) {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
        return asNonEmptyString(value, path + "." + key);
    }

    private static String optionalText(Map<String, Object> map, String key, String path) {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String text)) {
            throw new SpecException(path + "." + key + " must be a string");
        }
        return text;
    }

    private static String requireTextKey(Map<String, Object> map, String key, String path) {
        Object value = map.get(key);
        if (value == null) {
            throw new SpecException(path + "." + key + " is required");
        }
        if (!(value instanceof String text)) {
            throw new SpecException(path + "." + key + " must be a string");
        }
        if (text.isBlank()) {
            throw new SpecException(path + "." + key + " is required");
        }
        return text;
    }

    private static String asNonEmptyString(Object value, String path) {
        if (!(value instanceof String text)) {
            throw new SpecException(path + " must be a string");
        }
        if (text.isBlank()) {
            throw new SpecException(path + " must not be empty");
        }
        return text;
    }

    private static void requireRange(String value, String path, int maxLength) {
        if (value.length() > maxLength) {
            throw new SpecException(path + " must be 1.." + maxLength + " characters");
        }
    }

    private static Long asLong(Object value) {
        if (value instanceof Integer i) {
            return i.longValue();
        }
        if (value instanceof Long l) {
            return l;
        }
        if (value instanceof Short s) {
            return s.longValue();
        }
        return null;
    }
}
