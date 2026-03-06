package com.keboola.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for runtime backend switching via KEBOOLA USE BACKEND command.
 */
class BackendSwitchIT {

    private static final String DUCKDB_URL = "jdbc:keboola://localhost";

    @Test
    void switchToQueryServiceWithoutTokenThrowsError() throws Exception {
        Properties props = new Properties();
        props.setProperty("backend", "duckdb");

        try (Connection conn = DriverManager.getConnection(DUCKDB_URL, props)) {
            try (Statement stmt = conn.createStatement()) {
                SQLException ex = assertThrows(SQLException.class,
                        () -> stmt.execute("KEBOOLA USE BACKEND queryservice"));
                assertTrue(ex.getMessage().contains("token") || ex.getMessage().contains("Query Service"),
                        "Error should mention missing token or Query Service");
            }
        }
    }

    @Test
    void backendSwitchCommandReturnsConfirmation() throws Exception {
        Properties props = new Properties();
        props.setProperty("backend", "duckdb");

        try (Connection conn = DriverManager.getConnection(DUCKDB_URL, props)) {
            try (Statement stmt = conn.createStatement()) {
                // Switch to duckdb (already active, but should still work)
                boolean hasResult = stmt.execute("KEBOOLA USE BACKEND duckdb");
                assertTrue(hasResult, "Backend switch should return a ResultSet");

                try (ResultSet rs = stmt.getResultSet()) {
                    assertNotNull(rs);
                    assertTrue(rs.next());
                    assertEquals("OK", rs.getString("STATUS"));
                    assertEquals("duckdb", rs.getString("BACKEND"));
                    assertTrue(rs.getString("MESSAGE").contains("duckdb"));
                }
            }
        }
    }

    @Test
    void executeQueriesAfterBackendSwitch() throws Exception {
        Properties props = new Properties();
        props.setProperty("backend", "duckdb");

        try (Connection conn = DriverManager.getConnection(DUCKDB_URL, props)) {
            try (Statement stmt = conn.createStatement()) {
                // Create and populate in DuckDB
                stmt.execute("CREATE TABLE switch_test (id INTEGER)");
                stmt.execute("INSERT INTO switch_test VALUES (1), (2), (3)");

                // Switch to duckdb explicitly (no-op, but validates the flow)
                stmt.execute("KEBOOLA USE BACKEND duckdb");

                // Query should still work
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM switch_test")) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1));
                }
            }
        }
    }

    @Test
    void switchCommandCaseInsensitive() throws Exception {
        Properties props = new Properties();
        props.setProperty("backend", "duckdb");

        try (Connection conn = DriverManager.getConnection(DUCKDB_URL, props)) {
            try (Statement stmt = conn.createStatement()) {
                boolean hasResult = stmt.execute("keboola use backend DUCKDB");
                assertTrue(hasResult);
            }
        }
    }
}
