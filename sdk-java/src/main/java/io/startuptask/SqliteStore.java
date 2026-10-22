package io.startuptask;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class SqliteStore extends AbstractJdbcStore {

    private static final int BUSY_TIMEOUT_MS = 10_000;

    private final String jdbcUrl;
    private final AtomicBoolean walEnabled = new AtomicBoolean();

    public SqliteStore(String jdbcUrl) {
        super(Dialect.SQLITE);
        this.jdbcUrl = Objects.requireNonNull(jdbcUrl, "jdbcUrl");
    }

    public String jdbcUrl() {
        return jdbcUrl;
    }

    @Override
    protected Connection openConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcUrl);
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA busy_timeout=" + BUSY_TIMEOUT_MS);
            if (!walEnabled.get()) {
                enableWal(statement);
            }
        } catch (SQLException error) {
            connection.close();
            throw error;
        }
        return connection;
    }

    private void enableWal(Statement statement) throws SQLException {
        synchronized (walEnabled) {
            if (walEnabled.get()) {
                return;
            }
            try (ResultSet mode = statement.executeQuery("PRAGMA journal_mode")) {
                if (mode.next() && "wal".equalsIgnoreCase(mode.getString(1))) {
                    walEnabled.set(true);
                    return;
                }
            }
            statement.execute("PRAGMA journal_mode=WAL");
            walEnabled.set(true);
        }
    }
}
