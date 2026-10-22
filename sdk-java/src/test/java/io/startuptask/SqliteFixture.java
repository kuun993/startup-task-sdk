package io.startuptask;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

final class SqliteFixture implements AutoCloseable {

    record LockRow(String taskId, String ownerId, String acquiredAt, String expiresAt) {
    }

    record VersionRow(String taskId, String version, String status, String executedAt, String ownerId) {
    }

    private final Path directory;
    private final String jdbcUrl;
    private final SqliteStore store;

    SqliteFixture() {
        try {
            directory = Files.createTempDirectory("startup-task-sdk");
            jdbcUrl = "jdbc:sqlite:" + directory.resolve("state.db").toAbsolutePath();
            store = new SqliteStore(jdbcUrl);
            Schema.ensure(store);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    SqliteStore store() {
        return store;
    }

    String jdbcUrl() {
        return jdbcUrl;
    }

    DataSource dataSource() {
        return new SimpleDataSource(jdbcUrl, null, null);
    }

    Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    List<LockRow> locks() {
        return query("SELECT task_id, owner_id, acquired_at, expires_at FROM task_execution_lock ORDER BY task_id",
                rows -> new LockRow(rows.getString(1), rows.getString(2), rows.getString(3), rows.getString(4)));
    }

    List<VersionRow> versions() {
        return query("""
                        SELECT task_id, version, status, executed_at, owner_id
                          FROM task_version_record ORDER BY task_id, version""",
                rows -> new VersionRow(rows.getString(1), rows.getString(2), rows.getString(3),
                        rows.getString(4), rows.getString(5)));
    }

    void insertLock(String taskId, String ownerId, Instant acquiredAt, Instant expiresAt) {
        execute("INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at) VALUES (?, ?, ?, ?)",
                statement -> {
                    statement.setString(1, taskId);
                    statement.setString(2, ownerId);
                    statement.setString(3, SqlTypes.formatUtcTimestamp(acquiredAt));
                    statement.setString(4, SqlTypes.formatUtcTimestamp(expiresAt));
                });
    }

    void setLockOwner(String taskId, String ownerId) {
        execute("UPDATE task_execution_lock SET owner_id = ? WHERE task_id = ?", statement -> {
            statement.setString(1, ownerId);
            statement.setString(2, taskId);
        });
    }

    @Override
    public void close() {
        try (var paths = Files.walk(directory)) {
            paths.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception ignored) {
                    // best effort cleanup
                }
            });
        } catch (Exception ignored) {
            // best effort cleanup
        }
    }

    private <T> List<T> query(String sql, RowMapper<T> mapper) {
        List<T> result = new ArrayList<>();
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rows = statement.executeQuery()) {
            while (rows.next()) {
                result.add(mapper.map(rows));
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return result;
    }

    @FunctionalInterface
    private interface RowMapper<T> {
        T map(ResultSet rows) throws SQLException;
    }

    private void execute(String sql, StatementBinder binder) {
        try (Connection connection = openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @FunctionalInterface
    private interface StatementBinder {
        void bind(PreparedStatement statement) throws SQLException;
    }
}
