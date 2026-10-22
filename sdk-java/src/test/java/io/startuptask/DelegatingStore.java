package io.startuptask;

import java.sql.SQLException;
import java.time.Instant;

class DelegatingStore implements Store {

    protected final Store delegate;

    DelegatingStore(Store delegate) {
        this.delegate = delegate;
    }

    @Override
    public Dialect dialect() {
        return delegate.dialect();
    }

    @Override
    public boolean isDone(String taskId, String version) throws SQLException {
        return delegate.isDone(taskId, version);
    }

    @Override
    public LockOutcome acquireLock(String taskId, String ownerId, Instant acquiredAt, Instant expiresAt)
            throws SQLException {
        return delegate.acquireLock(taskId, ownerId, acquiredAt, expiresAt);
    }

    @Override
    public void recordVersion(String taskId, String version, String status, Instant executedAt, String ownerId)
            throws SQLException {
        delegate.recordVersion(taskId, version, status, executedAt, ownerId);
    }

    @Override
    public void releaseLock(String taskId, String ownerId) throws SQLException {
        delegate.releaseLock(taskId, ownerId);
    }

    @Override
    public void ensureSchema() throws SQLException {
        delegate.ensureSchema();
    }
}
