package io.startuptask;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

public final class MysqlStore extends AbstractJdbcStore {

    private final DataSource dataSource;

    public MysqlStore(DataSource dataSource) {
        super(Dialect.MYSQL);
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    public MysqlStore(String url, String user, String password) {
        this(new SimpleDataSource(url, user, password));
    }

    @Override
    protected Connection openConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
