package io.startuptask;

import java.sql.SQLException;
import java.time.Instant;

public interface Store {

    enum LockOutcome {
        ACQUIRED,
        TAKEOVER,
        NOT_ACQUIRED
    }

    Dialect dialect();

    boolean isDone(String taskId, String version) throws SQLException;

    LockOutcome acquireLock(String taskId, String ownerId, Instant acquiredAt, Instant expiresAt)
            throws SQLException;

    void recordVersion(String taskId, String version, String status, Instant executedAt, String ownerId)
            throws SQLException;

    void releaseLock(String taskId, String ownerId) throws SQLException;

    void ensureSchema() throws SQLException;
}
