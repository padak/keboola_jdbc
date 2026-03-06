package com.keboola.jdbc.command;

import com.keboola.jdbc.KeboolaConnection;
import com.keboola.jdbc.backend.DuckDbBackend;
import com.keboola.jdbc.backend.ExecutionResult;
import com.keboola.jdbc.backend.QueryServiceBackend;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

/**
 * Integration test for PushCommandHandler that uses real DuckDB
 * and a mocked Query Service backend.
 */
@ExtendWith(MockitoExtension.class)
class PushCommandIT {

    @Mock
    private KeboolaConnection connection;

    @Mock
    private QueryServiceBackend queryServiceBackend;

    private DuckDbBackend duckDbBackend;
    private PushCommandHandler handler;

    @BeforeEach
    void setUp() throws SQLException {
        duckDbBackend = new DuckDbBackend(":memory:");
        handler = new PushCommandHandler();

        lenient().when(connection.getQueryServiceBackend()).thenReturn(queryServiceBackend);
        lenient().when(connection.getDuckDbBackend()).thenReturn(duckDbBackend);

        // Query Service execute() returns an update count by default
        lenient().when(queryServiceBackend.execute(anyList())).thenReturn(ExecutionResult.forUpdateCount(0));
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (duckDbBackend != null) {
            duckDbBackend.close();
        }
    }

    @Test
    void push_sendsCreateTableAndInsert() throws SQLException {
        // Create a DuckDB table with test data
        duckDbBackend.execute(List.of(
                "CREATE TABLE my_data (id INTEGER, name VARCHAR, amount DOUBLE)"));
        duckDbBackend.execute(List.of(
                "INSERT INTO my_data VALUES (1, 'Alice', 100.50), (2, 'Bob', 200.75), (3, 'Charlie', NULL)"));

        // Execute PUSH
        ResultSet result = handler.execute("KEBOOLA PUSH TABLE my_data", connection);

        // Verify status result
        assertTrue(result.next());
        assertEquals("OK", result.getString("STATUS"));
        assertEquals("my_data", result.getString("TABLE"));
        assertEquals(3L, result.getLong("ROWS"));

        // Capture SQL sent to Query Service
        ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
        verify(queryServiceBackend, atLeast(2)).execute(captor.capture());

        List<List<String>> allCalls = captor.getAllValues();

        // First call should be CREATE TABLE
        String createSql = allCalls.get(0).get(0);
        assertTrue(createSql.contains("CREATE TABLE IF NOT EXISTS"), "Expected CREATE TABLE: " + createSql);
        assertTrue(createSql.contains("\"my_data\""), "Expected table name in CREATE: " + createSql);
        assertTrue(createSql.contains("VARCHAR"), "Expected VARCHAR columns: " + createSql);

        // Second call should be INSERT
        String insertSql = allCalls.get(1).get(0);
        assertTrue(insertSql.contains("INSERT INTO"), "Expected INSERT INTO: " + insertSql);
        assertTrue(insertSql.contains("'Alice'"), "Expected data value Alice: " + insertSql);
        assertTrue(insertSql.contains("'Bob'"), "Expected data value Bob: " + insertSql);
        assertTrue(insertSql.contains("NULL"), "Expected NULL value: " + insertSql);
    }

    @Test
    void push_withIntoClause_usesCorrectTarget() throws SQLException {
        // Create a DuckDB table
        duckDbBackend.execute(List.of("CREATE TABLE local_data (col1 VARCHAR)"));
        duckDbBackend.execute(List.of("INSERT INTO local_data VALUES ('value1')"));

        // Execute PUSH with INTO
        ResultSet result = handler.execute(
                "KEBOOLA PUSH TABLE local_data INTO in.c-main.target_table", connection);

        // Verify status
        assertTrue(result.next());
        assertEquals("OK", result.getString("STATUS"));
        assertEquals("in.c-main.target_table", result.getString("TABLE"));

        // Verify CREATE TABLE uses the correct schema.table reference
        ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
        verify(queryServiceBackend, atLeast(2)).execute(captor.capture());

        String createSql = captor.getAllValues().get(0).get(0);
        assertTrue(createSql.contains("\"in.c-main\".\"target_table\""),
                "Expected schema.table reference: " + createSql);
    }

    @Test
    void push_escapesQuotesInValues() throws SQLException {
        // Create a DuckDB table with data containing single quotes
        duckDbBackend.execute(List.of("CREATE TABLE quoted (text VARCHAR)"));
        duckDbBackend.execute(List.of("INSERT INTO quoted VALUES ('it''s a test')"));

        ResultSet result = handler.execute("KEBOOLA PUSH TABLE quoted", connection);

        assertTrue(result.next());
        assertEquals("OK", result.getString("STATUS"));

        // Verify the INSERT statement has escaped quotes
        ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
        verify(queryServiceBackend, atLeast(2)).execute(captor.capture());

        String insertSql = captor.getAllValues().get(1).get(0);
        assertTrue(insertSql.contains("it''s a test"), "Expected escaped quotes: " + insertSql);
    }

    @Test
    void push_withoutDuckDbBackend_throwsError() {
        when(connection.getDuckDbBackend()).thenReturn(null);

        SQLException ex = assertThrows(SQLException.class,
                () -> handler.execute("KEBOOLA PUSH TABLE my_data", connection));
        assertTrue(ex.getMessage().contains("DuckDB"), "Expected DuckDB error: " + ex.getMessage());
    }

    @Test
    void push_withoutQueryServiceBackend_throwsError() {
        when(connection.getQueryServiceBackend()).thenReturn(null);

        SQLException ex = assertThrows(SQLException.class,
                () -> handler.execute("KEBOOLA PUSH TABLE my_data", connection));
        assertTrue(ex.getMessage().contains("token"), "Expected token error: " + ex.getMessage());
    }
}
