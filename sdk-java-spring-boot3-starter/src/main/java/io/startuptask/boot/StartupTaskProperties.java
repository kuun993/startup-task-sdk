package io.startuptask.boot;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for the startup task starter.
 *
 * <pre>
 * startup-task:
 *   enabled: true
 *   owner-id: ${HOSTNAME:local}
 *   failure-policy: warn
 *   initialize-schema: false
 *   specs:
 *     - classpath:startup/*.yaml
 * </pre>
 */
@ConfigurationProperties(prefix = "startup-task")
public class StartupTaskProperties {

    /** Whether startup tasks run at all. The starter is inert unless this is true. */
    private boolean enabled;

    /** Owner id written to the lock table. Defaults to the host name (the pod name in Kubernetes). */
    private String ownerId = "${HOSTNAME:local}";

    /** What to do when a task fails: only log it, or abort application startup. */
    private FailurePolicy failurePolicy = FailurePolicy.WARN;

    /** Create the two coordination tables on first start. Prefer Flyway in production. */
    private boolean initializeSchema;

    /** Spec locations: classpath: or file: patterns; directories are expanded to *.yaml / *.yml. */
    private List<String> specs = new ArrayList<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public FailurePolicy getFailurePolicy() {
        return failurePolicy;
    }

    public void setFailurePolicy(FailurePolicy failurePolicy) {
        this.failurePolicy = failurePolicy;
    }

    public boolean isInitializeSchema() {
        return initializeSchema;
    }

    public void setInitializeSchema(boolean initializeSchema) {
        this.initializeSchema = initializeSchema;
    }

    public List<String> getSpecs() {
        return specs;
    }

    public void setSpecs(List<String> specs) {
        this.specs = specs;
    }

    public enum FailurePolicy {
        /** Log failures and let the application start. */
        WARN,
        /** Fail the application context when any task fails. */
        FAIL_FAST
    }
}
