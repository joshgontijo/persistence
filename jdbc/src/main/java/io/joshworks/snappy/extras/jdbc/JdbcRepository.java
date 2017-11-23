/*
 * Copyright 2017 Josue Gontijo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.joshworks.snappy.extras.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.plugin.dom.exception.InvalidStateException;

import javax.sql.DataSource;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Josh Gontijo on 3/25/17.
 */
public class JdbcRepository {

    private static final Logger logger = LoggerFactory.getLogger(JdbcRepository.class);

    private static final String PROPERTIES_NAME = "jdbc.properties";

    private static QueryRunner queryRunner;
    private static HikariDataSource dataSource;
    private static ExecutorService executor = Executors.newFixedThreadPool(5);

    public static synchronized void init(HikariDataSource ds) {
        dataSource = ds;
        queryRunner = new QueryRunner(ds);
    }

    public static synchronized void init() {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(PROPERTIES_NAME);
        if (is == null) {
            throw new InvalidStateException("Could not find " + PROPERTIES_NAME);
        }
        try {
            Properties props = new Properties();
            props.load(is);
            init(new HikariDataSource(new HikariConfig(props)));
        } catch (Exception e) {
            throw new JdbcException(e);
        }
    }

    public static synchronized void executor(ExecutorService exec) {
        Objects.requireNonNull(exec, "Executor cannot be null");
        executor.shutdown();
        executor = exec;
    }

    private static void shutdownExecutor() {
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void runScript(InputStream sqlFile, boolean autoCommit, boolean failOnError) {
        if (sqlFile == null) {
            throw new IllegalArgumentException("Script file must be provided");
        }
        if (dataSource == null) {
            throw new IllegalStateException("Datasource not initialized");
        }

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            ScriptRunner runner = new ScriptRunner(conn, autoCommit, failOnError);
            runner.runScript(new InputStreamReader(sqlFile));

        } catch (SQLException e) {
            if (conn != null)
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    throw new JdbcException("Error rolling back script execution", e1);
                }
            logger.error(e.getMessage(), e);

        } catch (IOException e) {
            throw new JdbcException("Could not read sql file", e);
        } finally {
            try {
                sqlFile.close();
            } catch (Exception ignore) {
            }
        }
    }

    public static synchronized void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            logger.info("Shutting down datasource");
            dataSource.close();
        }
        shutdownExecutor();
    }

    // BeanHandler<Exchange> exchangeBeanHandler = new BeanHandler<>(Exchange.class);
    // ResultSetHandler<List<Exchange>> h = new BeanListHandler<Exchange>(Exchange.class);
    public static int[] batch(String sql, Object[][] params) {
        try {
            return queryRunner.batch(sql, params);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public static <T> T query(String sql, ResultSetHandler<T> rsh, Object... params) {
        try {
            return queryRunner.query(sql, rsh, params);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public static <T> T query(String sql, ResultSetHandler<T> rsh) {
        try {
            return queryRunner.query(sql, rsh);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public static int update(String sql) {
        try {
            return queryRunner.update(sql);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public static int update(String sql, Object param) {
        try {
            return queryRunner.update(sql, param);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public static int update(String sql, Object... params) {
        try {
            return queryRunner.update(sql, params);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public static <T> T insert(String sql, ResultSetHandler<T> rsh) {
        try {
            return queryRunner.insert(sql, rsh);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public static <T> T insert(String sql, ResultSetHandler<T> rsh, Object... params) {
        try {
            return queryRunner.insert(sql, rsh, params);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public static <T> T insertBatch(String sql, ResultSetHandler<T> rsh, Object[][] params) {
        try {
            return queryRunner.insertBatch(sql, rsh, params);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public static DataSource getDataSource() {
        return queryRunner.getDataSource();
    }

    public static void fillStatement(PreparedStatement stmt, Object... params) {
        try {
            queryRunner.fillStatement(stmt, params);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public static void fillStatementWithBean(PreparedStatement stmt, Object bean, PropertyDescriptor[] properties) {
        try {
            queryRunner.fillStatementWithBean(stmt, bean, properties);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public static void fillStatementWithBean(PreparedStatement stmt, Object bean, String... propertyNames) {
        try {
            queryRunner.fillStatementWithBean(stmt, bean, propertyNames);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    //------- Async ---------
    public static CompletableFuture<int[]> asyncBatch(String sql, Object[][] params) {
        return CompletableFuture.supplyAsync(() -> JdbcRepository.batch(sql, params), executor);
    }

    public static <T> T asyncQuery(String sql, ResultSetHandler<T> rsh, Object... params) {
        return JdbcRepository.query(sql, rsh, params);
    }

    public static <T> CompletableFuture<T> asyncQuery(String sql, ResultSetHandler<T> rsh) {
        return CompletableFuture.supplyAsync(() -> JdbcRepository.query(sql, rsh), executor);
    }

    public static CompletableFuture<Integer> asyncUpdate(String sql) {
        return CompletableFuture.supplyAsync(() -> JdbcRepository.update(sql), executor);
    }

    public static CompletableFuture<Integer> asyncUpdate(String sql, Object param) {
        return CompletableFuture.supplyAsync(() -> JdbcRepository.update(sql, param), executor);
    }

    public static CompletableFuture<Integer> asyncUpdate(String sql, Object... params) {
        return CompletableFuture.supplyAsync(() -> JdbcRepository.update(sql, params), executor);
    }

    public static <T> CompletableFuture<T> asyncInsert(String sql, ResultSetHandler<T> rsh) {

        return CompletableFuture.supplyAsync(() -> JdbcRepository.insert(sql, rsh), executor);
    }

    public static <T> CompletableFuture<T> asyncIinsert(String sql, ResultSetHandler<T> rsh, Object... params) {
        return CompletableFuture.supplyAsync(() -> JdbcRepository.insert(sql, rsh, params), executor);
    }

    public static <T> CompletableFuture<T> asyncInsertBatch(String sql, ResultSetHandler<T> rsh, Object[][] params) {
        return CompletableFuture.supplyAsync(() -> JdbcRepository.insertBatch(sql, rsh, params), executor);
    }
}
