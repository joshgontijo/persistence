package io.joshworks.snappy.extras.jdbc;

import io.joshworks.snappy.extras.jdbc.stream.Row;
import org.junit.Test;

public class JdbcTest {

    @Test
    public void init() throws InterruptedException {
        Jdbc.init();

        Jdbc.update("CREATE TABLE PUBLIC.TEST_TABLE (" +
                "a INTEGER NOT NULL, " +
                "b CHAR(25), " +
                "c BIGINT(25), " +
                "d FLOAT(5), " +
                "e DOUBLE(5), " +
                "f DECIMAL(5,3), " +
                "g BOOLEAN, " +
                "h TIMESTAMP, " +
                "i VARCHAR(255), " +
                "PRIMARY KEY (a))");
        Jdbc.update("INSERT INTO PUBLIC.TEST_TABLE (a, b, c, d, e, f, g, h, i) VALUES (1, 'a', 1234567890123444, 15.5, 20.12, 45.666, true, '2015-10-01', 'josh1')");
        Jdbc.update("INSERT INTO PUBLIC.TEST_TABLE (a, b, c, d, e, f, g, h, i) VALUES (2, 'a', 1234567890123444, 15.5, 20.12, 45.666, true, '2015-10-01', 'josh1')");
        Jdbc.update("INSERT INTO PUBLIC.TEST_TABLE (a, b, c, d, e, f, g, h, i) VALUES (3, 'a', 1234567890123444, 15.5, 20.12, 45.666, true, '2015-10-01', 'josh1')");



        Jdbc.asyncQuery("SELECT * FROM PUBLIC.TEST_TABLE", Row::asMap, m -> {

        });

        Thread.sleep(5000);


    }

}