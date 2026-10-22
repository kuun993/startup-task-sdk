package io.startuptask;

import java.sql.SQLException;

public final class Schema {

    private Schema() {
    }

    public static void ensure(Store store) throws SQLException {
        store.ensureSchema();
    }
}
