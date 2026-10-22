package io.startuptask;

import java.time.Instant;

public interface Clock {

    Instant now();

    default long nanoTime() {
        return System.nanoTime();
    }
}
