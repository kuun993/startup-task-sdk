package io.startuptask;

import java.time.Instant;

public final class SystemClock implements Clock {

    public static final SystemClock INSTANCE = new SystemClock();

    private SystemClock() {
    }

    @Override
    public Instant now() {
        return Instant.now();
    }

    @Override
    public long nanoTime() {
        return System.nanoTime();
    }
}
