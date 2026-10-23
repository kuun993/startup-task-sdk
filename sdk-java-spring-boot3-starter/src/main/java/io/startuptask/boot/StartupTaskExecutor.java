package io.startuptask.boot;

import io.startuptask.Coordinator;
import io.startuptask.MysqlStore;
import io.startuptask.PostgresStore;
import io.startuptask.RunResult;
import io.startuptask.Schema;
import io.startuptask.SqliteStore;
import io.startuptask.Store;
import io.startuptask.TaskHandler;
import io.startuptask.TaskSpec;
import io.startuptask.TaskSpecLoader;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Runs the configured task specs once, using the application {@link DataSource}.
 *
 * <p>Separated from the Spring lifecycle hook so it can be invoked directly in tests.
 */
public class StartupTaskExecutor {

    private static final Logger LOG = Logger.getLogger(StartupTaskExecutor.class.getName());

    private final DataSource dataSource;
    private final StartupTaskProperties properties;
    private final ApplicationContext applicationContext;
    private final Environment environment;
    private final StartupTaskState state;
    private final AtomicBoolean ran = new AtomicBoolean();

    public StartupTaskExecutor(DataSource dataSource,
                               StartupTaskProperties properties,
                               ApplicationContext applicationContext,
                               Environment environment,
                               StartupTaskState state) {
        this.dataSource = dataSource;
        this.properties = properties;
        this.applicationContext = applicationContext;
        this.environment = environment;
        this.state = state;
    }

    /** Executes every configured spec. Runs at most once per application context. */
    public List<RunResult> execute() {
        if (!ran.compareAndSet(false, true)) {
            return state.getResults();
        }
        List<RunResult> results = List.of();
        try {
            Store store = createStore(dataSource);
            if (properties.isInitializeSchema()) {
                Schema.ensure(store);
            }
            Coordinator coordinator = new Coordinator(store, properties.getOwnerId());
            registerHandlers(coordinator);
            List<TaskSpec> specs = loadSpecs();
            if (specs.isEmpty()) {
                LOG.warning("startup-task: enabled but no specs matched " + properties.getSpecs());
            }
            results = coordinator.runAll(specs);
        } catch (Exception | LinkageError e) {
            // Configuration / infrastructure problems are always fatal: no task ran, so the
            // failure policy (which is about task outcomes) does not apply.
            state.recordError(e);
            throw new StartupTaskException("startup tasks could not run", e);
        }

        state.record(results);
        for (RunResult result : results) {
            LOG.info(() -> "startup-task: " + result);
        }
        if (state.isFailed()) {
            handleFailure("startup tasks failed: " + summarize(results), null);
        }
        return results;
    }

    private void handleFailure(String message, Throwable cause) {
        if (properties.getFailurePolicy() == StartupTaskProperties.FailurePolicy.FAIL_FAST) {
            throw new StartupTaskException(message, cause);
        }
        LOG.log(Level.WARNING, message, cause);
    }

    private static String summarize(List<RunResult> results) {
        StringBuilder text = new StringBuilder();
        for (RunResult result : results) {
            if (result.failed()) {
                if (!text.isEmpty()) {
                    text.append("; ");
                }
                text.append(result.taskId()).append(": ").append(result.error());
            }
        }
        return text.toString();
    }

    private void registerHandlers(Coordinator coordinator) {
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(StartupTaskHandler.class);
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            Object bean = entry.getValue();
            if (!(bean instanceof TaskHandler handler)) {
                throw new IllegalStateException("bean '" + entry.getKey() + "' is annotated with @StartupTaskHandler"
                        + " but does not implement io.startuptask.TaskHandler");
            }
            StartupTaskHandler annotation = bean.getClass().getAnnotation(StartupTaskHandler.class);
            coordinator.registerHandler(annotation.value(), handler);
        }
    }

    private List<TaskSpec> loadSpecs() throws IOException {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Map<String, TaskSpec> specs = new LinkedHashMap<>();
        for (String location : properties.getSpecs()) {
            List<Resource> resources = expand(resolver, location);
            for (Resource resource : resources) {
                TaskSpec spec = readSpec(resource);
                TaskSpec previous = specs.putIfAbsent(spec.taskId(), spec);
                if (previous != null) {
                    throw new IllegalStateException("duplicate task_id '" + spec.taskId()
                            + "' in startup-task.specs");
                }
            }
        }
        return new ArrayList<>(specs.values());
    }

    private List<Resource> expand(PathMatchingResourcePatternResolver resolver, String location) throws IOException {
        List<Resource> out = new ArrayList<>();
        Resource[] resources = resolver.getResources(location);
        Arrays.sort(resources, Comparator.comparing(resource -> String.valueOf(((Resource) resource).getFilename())));
        for (Resource resource : resources) {
            File file = existingFile(resource);
            if (file != null && file.isDirectory()) {
                try (Stream<Path> stream = Files.list(file.toPath())) {
                    stream.filter(path -> {
                                String name = path.getFileName().toString();
                                return name.endsWith(".yaml") || name.endsWith(".yml");
                            })
                            .sorted()
                            .forEach(path -> out.add(resolver.getResource("file:" + path.toAbsolutePath())));
                }
            } else {
                out.add(resource);
            }
        }
        return out;
    }

    private TaskSpec readSpec(Resource resource) throws IOException {
        String text;
        try (InputStream in = resource.getInputStream()) {
            text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        text = environment.resolvePlaceholders(text);
        TaskSpec raw = TaskSpecLoader.loadText(text);
        File file = existingFile(resource);
        Path baseDir = file == null ? null : file.toPath().toAbsolutePath().getParent();
        return new TaskSpec(raw.taskId(), raw.version(), raw.description(), raw.timeoutSeconds(),
                raw.executor(), raw.handler(), raw.config(), baseDir);
    }

    private static File existingFile(Resource resource) {
        try {
            return resource.getFile();
        } catch (IOException e) {
            return null;
        }
    }

    static Store createStore(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            String product = connection.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
            String url = connection.getMetaData().getURL();
            if (product.contains("postgresql") || product.contains("postgres")) {
                return new PostgresStore(dataSource);
            }
            if (product.contains("mysql") || product.contains("mariadb")) {
                return new MysqlStore(dataSource);
            }
            if (product.contains("sqlite")) {
                return new SqliteStore(url);
            }
            throw new IllegalStateException("startup-task: unsupported database '" + product
                    + "' (supported: PostgreSQL, MySQL/MariaDB, SQLite)");
        }
    }
}
