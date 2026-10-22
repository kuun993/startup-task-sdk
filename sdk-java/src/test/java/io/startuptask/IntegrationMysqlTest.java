package io.startuptask;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IntegrationMysqlTest extends AbstractStoreIntegrationTest {

    @Override
    protected String envName() {
        return "STARTUP_TASK_MYSQL_DSN";
    }

    @Override
    protected String jdbcSubprotocol() {
        return "mysql";
    }

    @Override
    protected Store createStore(JdbcDsn dsn) {
        return new MysqlStore(dsn.jdbcUrl(), dsn.user(), dsn.password());
    }

    @Test
    void schemaUsesDatetime6Columns() throws SQLException {
        assertEquals(2, count("""
                SELECT COUNT(*) FROM information_schema.tables
                 WHERE table_schema = DATABASE()
                   AND table_name IN ('task_execution_lock', 'task_version_record')"""));
        assertEquals("datetime", stringValue("""
                SELECT data_type FROM information_schema.columns
                 WHERE table_schema = DATABASE()
                   AND table_name = 'task_execution_lock' AND column_name = 'acquired_at'"""));
        assertEquals(6, count("""
                SELECT datetime_precision FROM information_schema.columns
                 WHERE table_schema = DATABASE()
                   AND table_name = 'task_execution_lock' AND column_name = 'acquired_at'"""));
        assertEquals(6, count("""
                SELECT datetime_precision FROM information_schema.columns
                 WHERE table_schema = DATABASE()
                   AND table_name = 'task_version_record' AND column_name = 'executed_at'"""));
        assertEquals(128, count("""
                SELECT character_maximum_length FROM information_schema.columns
                 WHERE table_schema = DATABASE()
                   AND table_name = 'task_execution_lock' AND column_name = 'owner_id'"""));
    }

    @Test
    void duplicatePrimaryKeyIsTreatedAsLockHeld() throws SQLException {
        String taskId = taskId("dupkey");
        Instant now = Instant.now();

        assertEquals(Store.LockOutcome.ACQUIRED,
                store.acquireLock(taskId, "it-pod-a", now, now.plusSeconds(300)));
        assertEquals(Store.LockOutcome.NOT_ACQUIRED,
                store.acquireLock(taskId, "it-pod-b", now, now.plusSeconds(300)));
        assertEquals(List.of("it-pod-a"), lockOwners(taskId));
    }

    @Test
    void upsertKeepsSingleRowForSameVersion() throws SQLException {
        String taskId = taskId("upsert");
        store.recordVersion(taskId, "1", "failed", Instant.now(), "it-pod-a");
        store.recordVersion(taskId, "1", "success", Instant.now(), "it-pod-b");

        assertEquals(1, count(
                "SELECT COUNT(*) FROM task_version_record WHERE task_id = ? AND version = '1'", taskId));
        assertEquals("success", stringValue(
                "SELECT status FROM task_version_record WHERE task_id = ? AND version = '1'", taskId));
        assertEquals("it-pod-b", stringValue(
                "SELECT owner_id FROM task_version_record WHERE task_id = ? AND version = '1'", taskId));
    }

    @Test
    void datetimeRoundTripsInUtc() throws SQLException {
        String taskId = taskId("utc");
        Instant instant = Instant.parse("2024-11-01T03:04:05.123456Z");
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));

            assertEquals(Store.LockOutcome.ACQUIRED,
                    store.acquireLock(taskId, "it-pod-a", instant, instant.plusSeconds(300)));
            assertEquals(instant, instantValue(
                    "SELECT acquired_at FROM task_execution_lock WHERE task_id = ?", taskId));

            store.recordVersion(taskId, "1", "success", instant, "it-pod-a");
            assertEquals(instant, instantValue(
                    "SELECT executed_at FROM task_version_record WHERE task_id = ?", taskId));
        } finally {
            TimeZone.setDefault(original);
        }
    }
}
