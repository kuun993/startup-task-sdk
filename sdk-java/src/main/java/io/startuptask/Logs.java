package io.startuptask;

import java.time.Instant;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public final class Logs {

    private static final String ROOT = "io.startuptask";

    private Logs() {
    }

    public static Logger get(Class<?> type) {
        return Logger.getLogger(type.getName());
    }

    public static void configure(Level level) {
        Logger logger = Logger.getLogger(ROOT);
        logger.setLevel(level);
        logger.setUseParentHandlers(false);
        for (Handler handler : logger.getHandlers()) {
            logger.removeHandler(handler);
        }
        ConsoleHandler console = new ConsoleHandler();
        console.setLevel(level);
        console.setFormatter(new SimpleFormatter() {
            @Override
            public String format(LogRecord record) {
                return Instant.ofEpochMilli(record.getMillis()) + " " + record.getLevel() + " "
                        + record.getMessage() + System.lineSeparator();
            }
        });
        logger.addHandler(console);
    }
}
