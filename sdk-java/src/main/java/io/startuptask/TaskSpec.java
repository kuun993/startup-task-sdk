package io.startuptask;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

public record TaskSpec(String taskId,
                       String version,
                       String description,
                       int timeoutSeconds,
                       Executor executor,
                       String handler,
                       Map<String, Object> config,
                       Path baseDir) {

    public static final int DEFAULT_TIMEOUT_SECONDS = 300;

    public enum Executor {
        HTTP("http"),
        CUSTOM("custom");

        private final String yamlName;

        Executor(String yamlName) {
            this.yamlName = yamlName;
        }

        public String yamlName() {
            return yamlName;
        }
    }

    public TaskSpec {
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(executor, "executor");
        config = config == null ? Map.of() : Values.immutableCopy(config);
    }

    public Map<String, Object> handlerConfig() {
        return Values.deepCopyMap(config);
    }
}
