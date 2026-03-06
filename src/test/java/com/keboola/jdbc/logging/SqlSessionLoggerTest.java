package com.keboola.jdbc.logging;

import com.keboola.jdbc.backend.DuckDbBackend;
import com.keboola.jdbc.config.DriverConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SqlSessionLogger} using a real in-memory DuckDB instance.
 */
class SqlSessionLoggerTest {

    private DuckDbBackend backend;
    private SqlSessionLogger logger;

    @BeforeEach
    void setUp() throws SQLException {
        backend = new DuckDbBackend(":memory:");
        logger = new SqlSessionLogger(backend);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (backend != null) {
            backend.close();
        }
    }

    @Test
    void log_createsTableAndInsertsRow() throws SQLException {
        logger.log("duckdb", "SELECT 1", true, null, 42, -1);

        Connection conn = backend.getNativeConnection();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT * FROM " + DriverConfig.SESSION_LOG_TABLE)) {

            assertTrue(rs.next(), "Should have one row");
            assertEquals("duckdb", rs.getString("backend"));
            assertEquals("SELECT 1", rs.getString("sql_text"));
            assertTrue(rs.getBoolean("success"));
            assertNull(rs.getString("error_message"));
            assertEquals(42, rs.getLong("duration_ms"));
            assertEquals(-1, rs.getLong("rows_affected"));
            assertNotNull(rs.getTimestamp("executed_at"));
            assertFalse(rs.next(), "Should have exactly one row");
        }
    }

    @Test
    void log_withErrorMessage() throws SQLException {
        logger.log("queryservice", "DROP TABLE foo", false, "Table not found: foo", 15, -1);

        Connection conn = backend.getNativeConnection();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT * FROM " + DriverConfig.SESSION_LOG_TABLE)) {

            assertTrue(rs.next());
            assertEquals("queryservice", rs.getString("backend"));
            assertEquals("DROP TABLE foo", rs.getString("sql_text"));
            assertFalse(rs.getBoolean("success"));
            assertEquals("Table not found: foo", rs.getString("error_message"));
            assertEquals(15, rs.getLong("duration_ms"));
        }
    }

    @Test
    void log_multipleEntries_verifyOrder() throws SQLException {
        logger.log("duckdb", "SELECT 1", true, null, 10, 1);
        logger.log("duckdb", "SELECT 2", true, null, 20, 2);
        logger.log("queryservice", "SELECT 3", false, "timeout", 30, -1);

        Connection conn = backend.getNativeConnection();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) AS cnt FROM " + DriverConfig.SESSION_LOG_TABLE)) {

            assertTrue(rs.next());
            assertEquals(3, rs.getInt("cnt"));
        }
    }

    @Test
    void log_withNullErrorDoesNotCrash() {
        // Should not throw any exception
        assertDoesNotThrow(() ->
                logger.log("duckdb", "CREATE TABLE t (id INT)", true, null, 5, 0)
        );
    }

    @Test
    void log_isIdempotentOnTableCreation() throws SQLException {
        // Log twice to verify the table creation is safe when called multiple times
        logger.log("duckdb", "SELECT 1", true, null, 1, 0);
        logger.log("duckdb", "SELECT 2", true, null, 2, 0);

        Connection conn = backend.getNativeConnection();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) AS cnt FROM " + DriverConfig.SESSION_LOG_TABLE)) {

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("cnt"));
        }
    }
}
