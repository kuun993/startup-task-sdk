package io.startuptask;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

public final class SqlTypes {

    private static final DateTimeFormatter UTC_TIMESTAMP =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS", Locale.ROOT).withZone(ZoneOffset.UTC);

    private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private SqlTypes() {
    }

    public static String formatUtcTimestamp(Instant instant) {
        return UTC_TIMESTAMP.format(instant.truncatedTo(ChronoUnit.MICROS));
    }

    public static void bindTimestamp(PreparedStatement statement, int index, Instant instant, Dialect dialect)
            throws SQLException {
        if (dialect == Dialect.SQLITE || dialect == Dialect.MYSQL) {
            // MySQL/MariaDB: mysql-connector-j's setTimestamp drops fractional seconds when writing
            // to a DATETIME(6) column on MariaDB 10.4 (reproduced with 8.4.0 / 9.2.0, both with
            // client-side and server-side prepared statements), so bind the UTC text
            // 'YYYY-MM-DD HH:MM:SS.ffffff' instead. The server still stores a native DATETIME,
            // microseconds are preserved, and the value does not depend on the JVM default zone.
            statement.setString(index, formatUtcTimestamp(instant));
        } else {
            statement.setTimestamp(index, Timestamp.from(instant), (Calendar) UTC_CALENDAR.clone());
        }
    }
}
