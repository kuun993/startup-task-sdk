package io.startuptask;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DialectTest {

    @Test
    void upsertDiffersByDialect() {
        assertTrue(Dialect.POSTGRES.upsertVersionRecord().contains("ON CONFLICT (task_id, version) DO UPDATE"));
        assertTrue(Dialect.SQLITE.upsertVersionRecord().contains("ON CONFLICT (task_id, version) DO UPDATE"));
        assertTrue(Dialect.MYSQL.upsertVersionRecord().contains("ON DUPLICATE KEY UPDATE"));
        assertFalse(Dialect.MYSQL.upsertVersionRecord().contains("ON CONFLICT"));
    }

    @Test
    void noDatabaseTimeFunctionsAreUsed() {
        for (Dialect dialect : Dialect.values()) {
            for (String sql : List.of(Dialect.Q1, Dialect.Q2, Dialect.Q3, dialect.upsertVersionRecord(),
                    Dialect.Q5)) {
                String lower = sql.toLowerCase();
                assertFalse(lower.contains("now()"), lower);
                assertFalse(lower.contains("current_timestamp"), lower);
                assertFalse(lower.contains("insert or ignore"), lower);
                assertFalse(lower.contains("insert ignore"), lower);
            }
        }
    }

    @Test
    void duplicateKeyDetection() {
        assertTrue(Dialect.POSTGRES.isDuplicateKey(new SQLException("duplicate key value", "23505")));
        assertFalse(Dialect.POSTGRES.isDuplicateKey(new SQLException("relation does not exist", "42P01")));
        assertTrue(Dialect.MYSQL.isDuplicateKey(new SQLException("Duplicate entry 'x' for key 'PRIMARY'", "23000", 1062)));
        assertFalse(Dialect.MYSQL.isDuplicateKey(new SQLException("cannot add foreign key constraint", "23000", 1215)));
        assertTrue(Dialect.SQLITE.isDuplicateKey(
                new SQLException("[SQLITE_CONSTRAINT_PRIMARYKEY] A PRIMARY KEY constraint failed "
                        + "(UNIQUE constraint failed: task_execution_lock.task_id)")));
        assertTrue(Dialect.SQLITE.isDuplicateKey(new SQLException("constraint failed", "23505")));
        assertFalse(Dialect.SQLITE.isDuplicateKey(new SQLException("[SQLITE_BUSY] database is locked")));
    }

    @Test
    void sqliteTimestampsAreUtcMicroseconds() {
        Instant instant = Instant.parse("2024-11-01T03:04:05.123456789Z");
        String formatted = SqlTypes.formatUtcTimestamp(instant);
        assertEquals("2024-11-01 03:04:05.123456", formatted);
        assertTrue(formatted.compareTo("2024-11-01 03:04:05.123455") > 0);
        assertTrue(formatted.compareTo("2024-11-01 03:04:06.000000") < 0);
    }
}
