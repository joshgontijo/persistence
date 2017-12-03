package io.joshworks.snappy.extras.jdbc;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JdbcTest {

    @BeforeClass
    public static void setUp() {
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
    }

    @AfterClass
    public static void shutdown() {
        System.out.println("Releasing connection");
        Jdbc.close();
        System.out.println("Connection released");
    }

    @After
    public void cleanup() {
        Jdbc.update("DELETE FROM PUBLIC.TEST_TABLE");
    }

    @Test
    public void typeMapper() throws InterruptedException {
        Jdbc.update("INSERT INTO PUBLIC.TEST_TABLE (a, b, c, d, e, f, g, h, i) VALUES (1, 'a', 1234567890123444, 15.5, 20.12, 45.666, true, '2015-10-01', 'josh1')");
        Jdbc.update("INSERT INTO PUBLIC.TEST_TABLE (a, b, c, d, e, f, g, h, i) VALUES (2, 'a', 1234567890123444, 15.5, 20.12, 45.666, true, '2015-10-01', 'josh1')");
        Jdbc.update("INSERT INTO PUBLIC.TEST_TABLE (a, b, c, d, e, f, g, h, i) VALUES (3, 'a', 1234567890123444, 15.5, 20.12, 45.666, true, '2015-10-01', 'josh1')");

        final CountDownLatch latch = new CountDownLatch(3);
        Jdbc.streamType("SELECT a,b,c,d,e,f,g,h,i FROM PUBLIC.TEST_TABLE", TableData.class, m -> {
            System.out.println(m);
            latch.countDown();
        });

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("Wait timeout");
        }
    }

    @Test
    public void streamRows() throws InterruptedException {
        Jdbc.update("INSERT INTO PUBLIC.TEST_TABLE (a, b, c, d, e, f, g, h, i) VALUES (1, 'a', 1234567890123444, 15.5, 20.12, 45.666, true, '2015-10-01', 'josh1')");


        final CountDownLatch latch = new CountDownLatch(1);
        Jdbc.stream("SELECT * FROM PUBLIC.TEST_TABLE", row -> {
            System.out.println(row);
            assertNotNull(row);
            latch.countDown();
        });

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("Wait timeout");
        }
    }

    @Test
    public void asyncQuery() throws InterruptedException {
        Jdbc.update("INSERT INTO PUBLIC.TEST_TABLE (a, b, c, d, e, f, g, h, i) VALUES (1, 'a', 1234567890123444, 15.5, 20.12, 45.666, true, '2015-10-01', 'josh1')");


        final CountDownLatch latch = new CountDownLatch(1);
        Jdbc.streamMapping("SELECT * FROM PUBLIC.TEST_TABLE", Row::asMap, m -> {
            assertEquals(9, m.size());
            latch.countDown();
        });

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("Wait timeout");
        }
    }

    @Test
    public void insertAsync() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        Jdbc.asyncInsert("INSERT INTO PUBLIC.TEST_TABLE (a, b, c, d, e, f, g, h, i) VALUES (1, 'a', 1234567890123444, 15.5, 20.12, 45.666, true, '2015-10-01', 'josh1')",
                Throwable::printStackTrace,
                r -> {
                    latch.countDown();
                    return null;
                });

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("Wait timeout");
        }

        Rows rows = Jdbc.query("SELECT * FROM PUBLIC.TEST_TABLE");
        assertEquals(1, rows.size());


    }

    @Test
    public void rows() throws InterruptedException {
        Jdbc.update("INSERT INTO PUBLIC.TEST_TABLE (a, b, c, d, e, f, g, h, i) VALUES (1, 'a', 1234567890123444, 15.5, 20.12, 45.666, true, '2015-10-01', 'josh1')");

        final CountDownLatch latch = new CountDownLatch(1);
        Jdbc.asyncQuery("SELECT * FROM PUBLIC.TEST_TABLE", rows -> {
            assertEquals(1, rows.size());
            assertTrue(rows.first().isPresent());
            latch.countDown();
        });

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("Wait timeout");
        }
    }


}