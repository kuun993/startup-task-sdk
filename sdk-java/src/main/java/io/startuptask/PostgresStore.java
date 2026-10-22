package io.startuptask;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

public final class PostgresStore extends AbstractJdbcStore {

    private final DataSource dataSource;

    public PostgresStore(DataSource dataSource) {
        super(Dialect.POSTGRES);
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    public PostgresStore(String url, String user, String password) {
        this(new SimpleDataSource(url, user, password));
    }

    @Override
    protected Connection openConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
