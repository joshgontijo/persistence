package io.joshworks.snappy.extras.jdbc;

import org.apache.commons.dbutils.QueryRunner;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Objects;

class ResultSetIterator extends QueryRunner implements Iterator<Row>, Closeable {

    private final Object[] params;
    private ResultSet rs;
    private PreparedStatement ps;
    private Connection connection;
    private String sql;
    private final int fetchSize; //default from JDBC

    ResultSetIterator(Connection connection, String sql,  Object[] params) {
        this(connection, sql, 0, params);
    }

    ResultSetIterator(Connection connection, String sql, int fetchSize, Object[] params) {
        Objects.requireNonNull(connection, "Connection must be provided");
        Objects.requireNonNull(sql, "SQL query must be provided");
        this.fetchSize = fetchSize;
        this.connection = connection;
        this.sql = sql;
        this.params = params == null ? new Object[]{} : params;
    }

    private void init() {
        try {
            this.ps = this.prepareStatement(connection, sql);
            ps.setFetchSize(fetchSize);
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