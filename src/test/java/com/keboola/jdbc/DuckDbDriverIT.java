package com.keboola.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the DuckDB backend.
 * These tests run without any cloud dependencies - purely local DuckDB.
 */
class DuckDbDriverIT {

    private static final String DUCKDB_URL = "jdbc:keboola://localhost";

    private Properties duckdbProps() {
        Properties props = new Properties();
        props.setProperty("backend", "duckdb");
        return props;
    }

    private Properties duckdbProps(String duckdbPath) {
        Properties props = duckdbProps();
        props.setProperty("duckdbPath", duckdbPath);
        return props;
    }

    @Test
    void connectAndExecuteCreateInsertSelect() throws Exception {
        try (Connection conn = DriverManager.getConnection(DUCKDB_URL, duckdbProps())) {
            assertNotNull(conn);
            assertFalse(conn.isClosed());

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)");
                stmt.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')");

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_table ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                    assertEquals("Alice", rs.getString(2));

                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1));
                    assertEquals("Bob", rs.getString(2));

                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    void metadataOperations() throws Exception {
        try (Connection conn = DriverManager.getConnection(DUCKDB_URL, duckdbProps())) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE SCHEMA test_schema");
                stmt.execute("CREATE TABLE test_schema.my_table (col1 INTEGER, col2 VARCHAR)");
            }

            DatabaseMetaData meta = conn.getMetaData();
            assertNotNull(meta);

            // getCatalogs
            try (ResultSet rs = meta.getCatalogs()) {
                assertTrue(rs.next(), "Should have at least one catalog");
            }

            // getSchemas
            try (ResultSet rs = meta.getSchemas()) {
                boolean foundTestSchema = false;
                while (rs.next()) {
                    if ("test_schema".equals(rs.getString("TABLE_SCHEM"))) {
                        foundTestSchema = true;
                    }
                }
                assertTrue(foundTestSchema, "Should find test_schema");
            }

            // getTables
            try (ResultSet rs = meta.getTables(null, "test_schema", "%", null)) {
                assertTrue(rs.next(), "Should find my_table");
                assertEquals("my_table", rs.getString("TABLE_NAME"));
            }

            // getColumns
            try (ResultSet rs = meta.getColumns(null, "test_schema", "my_table", "%")) {
                assertTrue(rs.next());
                assertEquals("col1", rs.getString("COLUMN_NAME"));
                assertTrue(rs.next());
                assertEquals("col2", rs.getString("COLUMN_NAME"));
            }
        }
    }

    @Test
    void persistentDuckDbFile(@TempDir Path tempDir) throws Exception {
        String dbPath = tempDir.resolve("test.duckdb").toString();

        // First connection: create table and insert data
        try (Connection conn = DriverManager.getConnection(DUCKDB_URL, duckdbProps(dbPath))) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE persistent_test (value INTEGER)");
                stmt.execute("INSERT INTO persistent_test VALUES (42)");
            }
        }

        // Second connection: read data from the same file
        try (Connection conn = DriverManager.getConnection(DUCKDB_URL, duckdbProps(dbPath))) {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT value FROM persistent_test")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
            }
        }
    }

    @Test
    void inMemoryModeDataLostBetweenConnections() throws Exception {
        // First connection: create table
        try (Connection conn = DriverManager.getConnection(DUCKDB_URL, duckdbProps())) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE ephemeral_test (value INTEGER)");
                stmt.execute("INSERT INTO ephemeral_test VALUES (99)");
            }
        }

        // Second connection: table should not exist
        try (Connection conn = DriverManager.getConnection(DUCKDB_URL, duckdbProps())) {
            try (Statement stmt = conn.createStatement()) {
                assertThrows(Exception.class, () ->
                        stmt.executeQuery("SELECT * FROM ephemeral_test"));
            }
        }
    }

    @Test
    void driverProductInfo() throws Exception {
        try (Connection conn = DriverManager.getConnection(DUCKDB_URL, duckdbProps())) {
            DatabaseMetaData meta = conn.getMetaData();
            assertEquals("Keboola", meta.getDatabaseProductName());
            assertEquals("Keboola JDBC Driver", meta.getDriverName());
        }
    }
}
