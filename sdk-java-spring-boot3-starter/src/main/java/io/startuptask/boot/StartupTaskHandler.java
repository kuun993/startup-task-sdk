package io.startuptask.boot;

import org.springframework.stereotype.Component;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a bean as a custom task handler. The bean must implement
 * {@link io.startuptask.TaskHandler} and is registered under {@link #value()}.
 *
 * <p>Meta-annotated with {@link Component}, so a class in a scanned package is picked up
 * automatically.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface StartupTaskHandler {

    /** Handler name as referenced by {@code execution.handler} in the task YAML. */
    String value();
}
