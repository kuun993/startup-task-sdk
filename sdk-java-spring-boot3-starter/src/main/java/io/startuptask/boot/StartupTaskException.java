package io.startuptask.boot;

/** Thrown when startup tasks fail and {@code startup-task.failure-policy} is {@code fail-fast}. */
public class StartupTaskException extends RuntimeException {

    public StartupTaskException(String message) {
        super(message);
    }

    public StartupTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
