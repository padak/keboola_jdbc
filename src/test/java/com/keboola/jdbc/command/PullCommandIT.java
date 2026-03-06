package com.keboola.jdbc.command;

import com.keboola.jdbc.ArrayResultSet;
import com.keboola.jdbc.KeboolaConnection;
import com.keboola.jdbc.backend.DuckDbBackend;
import com.keboola.jdbc.backend.ExecutionResult;
import com.keboola.jdbc.backend.QueryServiceBackend;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

/**
 * Integration test for PullCommandHandler that uses real DuckDB
 * and a mocked Query Service backend.
 */
@ExtendWith(MockitoExtension.class)
class PullCommandIT {

    @Mock
    private KeboolaConnection connection;

    @Mock
    private QueryServiceBackend queryServiceBackend;

    private DuckDbBackend duckDbBackend;
    private PullCommandHandler handler;

    @BeforeEach
    void setUp() throws SQLException {
        duckDbBackend = new DuckDbBackend(":memory:");
        handler = new PullCommandHandler();

        when(connection.getQueryServiceBackend()).thenReturn(queryServiceBackend);
        when(connection.getDuckDbBackend()).thenReturn(duckDbBackend);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (duckDbBackend != null) {
            duckDbBackend.close();
        }
    }

    @Test
    void pullQuery_materializesDataIntoDuckDb() throws SQLException {
        // Create a mock ResultSet that the Query Service would return
        ResultSet sourceRs = createMockResultSet(
                Arrays.asList("id", "name", "amount"),
                Arrays.asList(Types.INTEGER, Types.VARCHAR, Types.DOUBLE),
                Arrays.asList("INTEGER", "VARCHAR", "DOUBLE"),
                Arrays.asList(
                        Arrays.asList("1", "Alice", "100.50"),
                        Arrays.asList("2", "Bob", "200.75"),
                        Arrays.asList("3", "Charlie", null)
                )
        );

        ExecutionResult qsResult = ExecutionResult.forResultSet(sourceRs);
        when(queryServiceBackend.execute(anyList())).thenReturn(qsResult);

        // Execute PULL QUERY
        ResultSet result = handler.execute(
                "KEBOOLA PULL QUERY SELECT id, name, amount FROM \"in.c-main\".\"sales\" INTO sales_data",
                connection);

        // Verify confirmation
        assertTrue(result.next());
        assertEquals("OK", result.getString("STATUS"));
        assertEquals("sales_data", result.getString("TABLE"));
        assertEquals(3L, result.getLong("ROWS"));

        // Verify data was actually inserted into DuckDB
        ExecutionResult duckResult = duckDbBackend.execute(
                List.of("SELECT * FROM sales_data ORDER BY id"));
        assertTrue(duckResult.hasResultSet());

        ResultSet duckRs = duckResult.getResultSet();
        assertTrue(duckRs.next());
        assertEquals("1", duckRs.getString(1));
        assertEquals("Alice", duckRs.getString(2));
        assertEquals("100.50", duckRs.getString(3));

        assertTrue(duckRs.next());
        assertEquals("2", duckRs.getString(1));
        assertEquals("Bob", duckRs.getString(2));

        assertTrue(duckRs.next());
        assertEquals("3", duckRs.getString(1));
        assertEquals("Charlie", duckRs.getString(2));
        assertNull(duckRs.getString(3));

        assertFalse(duckRs.next());
        duckRs.close();
    }

    @Test
    void pullTable_createsCorrectQuery() throws SQLException {
        // Verify that PULL TABLE generates the right SELECT
        ResultSet sourceRs = createMockResultSet(
                Arrays.asList("col1"),
                Arrays.asList(Types.VARCHAR),
                Arrays.asList("VARCHAR"),
                Arrays.asList(Arrays.asList("value1"))
        );
        ExecutionResult qsResult = ExecutionResult.forResultSet(sourceRs);
        when(queryServiceBackend.execute(anyList())).thenReturn(qsResult);

        handler.execute("KEBOOLA PULL TABLE in.c-main.orders INTO my_orders", connection);

        // Verify the query sent to Query Service
        verify(queryServiceBackend).execute(
                List.of("SELECT * FROM \"in.c-main\".\"orders\""));
    }

    @Test
    void pullWithoutToken_throwsError() throws SQLException {
        when(connection.getQueryServiceBackend()).thenReturn(null);

        SQLException ex = assertThrows(SQLException.class,
                () -> handler.execute("KEBOOLA PULL QUERY SELECT 1 INTO test", connection));
        assertTrue(ex.getMessage().contains("token"));
    }

    @Test
    void pullReplacesExistingTable() throws SQLException {
        // First: create a table directly in DuckDB
        duckDbBackend.execute(List.of("CREATE TABLE replaceable (old_col VARCHAR)"));
        duckDbBackend.execute(List.of("INSERT INTO replaceable VALUES ('old_data')"));

        // Pull new data (should DROP + CREATE)
        ResultSet sourceRs = createMockResultSet(
                Arrays.asList("new_col"),
                Arrays.asList(Types.VARCHAR),
                Arrays.asList("VARCHAR"),
                Arrays.asList(Arrays.asList("new_data"))
        );
        when(queryServiceBackend.execute(anyList())).thenReturn(ExecutionResult.forResultSet(sourceRs));

        handler.execute("KEBOOLA PULL QUERY SELECT 1 INTO replaceable", connection);

        // Verify old table was replaced
        ExecutionResult result = duckDbBackend.execute(List.of("SELECT * FROM replaceable"));
        ResultSet rs = result.getResultSet();
        assertTrue(rs.next());
        assertEquals("new_data", rs.getString(1));
        assertFalse(rs.next());
        rs.close();
    }

    // --- Helper: create a mock ResultSet with metadata ---

    private ResultSet createMockResultSet(List<String> columnNames, List<Integer> columnTypes,
                                           List<String> typeNames, List<List<String>> rows)
            throws SQLException {
        // Build a real ArrayResultSet with the data
        List<String> cols = columnNames;
        List<List<Object>> dataRows = new java.util.ArrayList<>();
        for (List<String> row : rows) {
            List<Object> objRow = new java.util.ArrayList<>(row);
            dataRows.add(objRow);
        }
        ArrayResultSet ars = new ArrayResultSet(cols, dataRows);

        // But we need proper metadata with JDBC types, so wrap it
        // Actually, ArrayResultSet.getMetaData() returns metadata with Types.VARCHAR
        // We need to create a custom wrapper that provides correct type info
        // For simplicity, use a Mockito spy approach with a real cursor

        ResultSet mockRs = mock(ResultSet.class);
        ResultSetMetaData mockMeta = mock(ResultSetMetaData.class);

        when(mockRs.getMetaData()).thenReturn(mockMeta);
        when(mockMeta.getColumnCount()).thenReturn(columnNames.size());

        for (int i = 0; i < columnNames.size(); i++) {
            int col = i + 1;
            when(mockMeta.getColumnName(col)).thenReturn(columnNames.get(i));
            when(mockMeta.getColumnType(col)).thenReturn(columnTypes.get(i));
            when(mockMeta.getColumnTypeName(col)).thenReturn(typeNames.get(i));
            when(mockMeta.getPrecision(col)).thenReturn(0);
            when(mockMeta.getScale(col)).thenReturn(0);
        }

        // Set up next() and getString() to iterate through rows
        final int[] rowIndex = {-1};
        when(mockRs.next()).thenAnswer(inv -> {
            rowIndex[0]++;
            return rowIndex[0] < rows.size();
        });

        for (int i = 0; i < columnNames.size(); i++) {
            final int colIdx = i;
            when(mockRs.getString(i + 1)).thenAnswer(inv -> {
                if (rowIndex[0] >= 0 && rowIndex[0] < rows.size()) {
                    return rows.get(rowIndex[0]).get(colIdx);
                }
                return null;
            });
        }

        return mockRs;
    }
}
