package io.startuptask;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Common JDBC base class for the dialect-specific stores. Transaction semantics: every store call
 * opens its own connection and does exactly one thing, so it is treated as an independent unit of
 * work. When the connection has autocommit disabled this class commits the successful work and
 * rolls back the failed work; otherwise a pooled connection with autocommit off would neither
 * persist the statements nor clean up the aborted/open transaction left behind by a failed
 * statement (PostgreSQL marks the transaction aborted and the following Q3 would fail with 25P02).
 */
public abstract class AbstractJdbcStore implements Store {

    private static final Logger LOG = Logs.get(AbstractJdbcStore.class);

    private final Dialect dialect;

    protected AbstractJdbcStore(Dialect dialect) {
        this.dialect = Objects.requireNonNull(dialect, "dialect");
    }

    protected abstract Connection openConnection() throws SQLException;

    @Override
    public final Dialect dialect() {
        return dialect;
    }

    @Override
    public boolean isDone(String taskId, String version) throws SQLException {
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(Dialect.Q1)) {
            statement.setString(1, taskId);
            statement.setString(2, version);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    @Override
    public LockOutcome acquireLock(String taskId, String ownerId, Instant acquiredAt, Instant expiresAt)
            throws SQLException {
        try (Connection connection = openConnection()) {
            try (PreparedStatement insert = connection.prepareStatement(Dialect.Q2)) {
                insert.setString(1, taskId);
                insert.setString(2, ownerId);
                SqlTypes.bindTimestamp(insert, 3, acquiredAt, dialect);
                SqlTypes.bindTimestamp(insert, 4, expiresAt, dialect);
                insert.executeUpdate();
                commit(connection);
                return LockOutcome.ACQUIRED;
            } catch (SQLException error) {
                if (!dialect.isDuplicateKey(error)) {
                    rollback(connection);
                    throw error;
                }
                // A primary-key conflict is an expected branch (the lock is already held). On a
                // connection with autocommit disabled this failed statement leaves an aborted
                // transaction (PostgreSQL), so it must be rolled back before Q3 can attempt the
                // expired-lock takeover.
                rollback(connection);
            }
            try (PreparedStatement takeover = connection.prepareStatement(Dialect.Q3)) {
                takeover.setString(1, ownerId);
                SqlTypes.bindTimestamp(takeover, 2, acquiredAt, dialect);
                SqlTypes.bindTimestamp(takeover, 3, expiresAt, dialect);
                takeover.setString(4, taskId);
                SqlTypes.bindTimestamp(takeover, 5, acquiredAt, dialect);
                int updated = takeover.executeUpdate();
                commit(connection);
                return updated == 1 ? LockOutcome.TAKEOVER : LockOutcome.NOT_ACQUIRED;
            } catch (SQLException error) {
                rollback(connection);
                throw error;
            }
        }
    }

    @Override
    public void recordVersion(String taskId, String version, String status, Instant executedAt, String ownerId)
            throws SQLException {
        try (Connection connection = openConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(dialect.upsertVersionRecord())) {
                statement.setString(1, taskId);
                statement.setString(2, version);
                statement.setString(3, status);
                SqlTypes.bindTimestamp(statement, 4, executedAt, dialect);
                statement.setString(5, ownerId);
                statement.executeUpdate();
                commit(connection);
            } catch (SQLException error) {
                rollback(connection);
                throw error;
            }
        }
    }

    @Override
    public void releaseLock(String taskId, String ownerId) throws SQLException {
        try (Connection connection = openConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(Dialect.Q5)) {
                statement.setString(1, taskId);
                statement.setString(2, ownerId);
                statement.executeUpdate();
                commit(connection);
            } catch (SQLException error) {
                rollback(connection);
                throw error;
            }
        }
    }

    @Override
    public void ensureSchema() throws SQLException {
        try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
            try {
                for (String ddl : dialect.ddl()) {
                    statement.execute(ddl);
                }
                commit(connection);
            } catch (SQLException error) {
                rollback(connection);
                throw error;
            }
        }
    }

    private static void commit(Connection connection) throws SQLException {
        if (!connection.getAutoCommit()) {
            connection.commit();
        }
    }

    private static void rollback(Connection connection) {
        try {
            if (!connection.getAutoCommit()) {
                connection.rollback();
            }
        } catch (SQLException error) {
            LOG.log(Level.WARNING, "transaction rollback failed", error);
        }
    }
}
