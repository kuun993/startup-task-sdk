package io.startuptask;

import java.util.Map;

@FunctionalInterface
public interface TaskHandler {

    void handle(Map<String, Object> config) throws Exception;
}
