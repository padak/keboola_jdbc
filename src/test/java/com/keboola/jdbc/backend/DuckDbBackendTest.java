package com.keboola.jdbc.backend;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link DuckDbBackend} using an in-memory DuckDB instance.
 */
class DuckDbBackendTest {

    private DuckDbBackend backend;

    @BeforeEach
    void setUp() throws SQLException {
        backend = new DuckDbBackend(":memory:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (backend != null) {
            backend.close();
        }
    }

    @Test
    void createInMemoryAndExecuteDDL() throws SQLException {
        // Executing a CREATE TABLE should succeed without throwing
        ExecutionResult result = backend.execute(
                List.of("CREATE TABLE test_ddl (id INTEGER, name VARCHAR)")
        );

        assertNotNull(result, "ExecutionResult should not be null after DDL");
    }

    @Test
    void executeInsertAndSelect() throws SQLException {
        // Setup table and insert rows
        backend.execute(List.of("CREATE TABLE users (id INTEGER, name VARCHAR)"));
        backend.execute(List.of(
                "INSERT INTO users VALUES (1, 'Alice')",
                "INSERT INTO users VALUES (2, 'Bob')"
        ));

        // Select all rows
        ExecutionResult result = backend.execute(List.of("SELECT * FROM users ORDER BY id"));

        assertTrue(result.hasResultSet(), "SELECT should return a ResultSet");
        ResultSet rs = result.getResultSet();
        assertNotNull(rs, "ResultSet should not be null");

        // Verify first row
        assertTrue(rs.next(), "Should have first row");
        assertEquals(1, rs.getInt("id"));
        assertEquals("Alice", rs.getString("name"));

        // Verify second row
        assertTrue(rs.next(), "Should have second row");
        assertEquals(2, rs.getInt("id"));
        assertEquals("Bob", rs.getString("name"));

        // No more rows
        assertFalse(rs.next(), "Should have no more rows");

        rs.close();
    }

    @Test
    void executeReturnsCorrectResultTypes() throws SQLException {
        // DDL should return update count, not a ResultSet
        ExecutionResult ddlResult = backend.execute(
                List.of("CREATE TABLE types_test (id INTEGER)")
        );

        assertFalse(ddlResult.hasResultSet(), "CREATE TABLE should not return a ResultSet");
        assertNull(ddlResult.getResultSet(), "ResultSet should be null for DDL");

        // SELECT should return a ResultSet
        ExecutionResult selectResult = backend.execute(
                List.of("SELECT 1 AS value")
        );

        assertTrue(selectResult.hasResultSet(), "SELECT should return a ResultSet");
        assertNotNull(selectResult.getResultSet(), "ResultSet should not be null for SELECT");
        assertEquals(-1, selectResult.getUpdateCount(), "Update count should be -1 for a query");

        selectResult.getResultSet().close();
    }

    @Test
    void getCurrentCatalogAndSchema() throws SQLException {
        String catalog = backend.getCurrentCatalog();
        String schema = backend.getCurrentSchema();

        assertNotNull(catalog, "Current catalog should not be null for DuckDB");
        assertNotNull(schema, "Current schema should not be null for DuckDB");
    }

    @Test
    void cancelDoesNotThrow() throws SQLException {
        // cancel() should not throw even when no query is running
        assertDoesNotThrow(() -> backend.cancel());
    }

    @Test
    void closeReleasesResources() throws SQLException {
        backend.close();

        // After close, operations on the underlying connection should fail
        assertThrows(SQLException.class, () -> backend.execute(List.of("SELECT 1")),
                "Executing after close should throw SQLException");

        // Prevent double-close in tearDown
        backend = null;
    }
}
