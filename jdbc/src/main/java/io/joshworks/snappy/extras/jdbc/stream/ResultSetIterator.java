package io.joshworks.snappy.extras.jdbc.stream;

import io.joshworks.snappy.extras.jdbc.JdbcException;
import org.apache.commons.dbutils.QueryRunner;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class ResultSetIterator extends QueryRunner implements Iterator<Row>, Closeable {

    private final Object[] params;
    private ResultSet rs;
    private PreparedStatement ps;
    private Connection connection;
    private String sql;

    public ResultSetIterator(Connection connection, String sql, Object[] params) {
        assert connection != null;
        assert sql != null;
        this.connection = connection;
        this.sql = sql;
        this.params = params == null ? new Object[]{} : params;
    }

    public void init() {
        try {
            this.ps = this.prepareStatement(connection, sql);
            this.fillStatement(this.ps, params);
            rs = this.wrap(this.ps.executeQuery());

        } catch (SQLException e) {
            close();
            throw new JdbcException(e);
        }
    }

    @Override
    public boolean hasNext() {
        if (ps == null) {
            init();
        }
        try {
            boolean hasNext = rs.next();
            if (!hasNext) {
                close();
            }
            return hasNext;
        } catch (SQLException e) {
            close();
            throw new JdbcException(e);
        }

    }

    @Override
    public void close() {
        System.out.println("Closing");
        if (rs != null) {
            try {
                rs.close();
                rs = null;
            } catch (SQLException e) {

            }
        }
        try {
            if (ps != null) {
                ps.close();
                ps = null;
            }
        } catch (SQLException e) {
            //nothing we can do here
        }
    }

    @Override
    public Row next() {
        return Row.fromResultSet(rs);
    }
}