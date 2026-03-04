package com.keboola.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ArrayResultSet - verifies in-memory ResultSet navigation and data access.
 */
class ArrayResultSetTest {

    // -------------------------------------------------------------------------
    // Helper factory methods
    // -------------------------------------------------------------------------

    private ArrayResultSet buildResultSet(List<String> columns, List<List<Object>> rows) {
        return new ArrayResultSet(columns, rows);
    }

    private ArrayResultSet buildSingleRowResultSet() {
        List<String> columns = Arrays.asList("id", "name", "active");
        List<List<Object>> rows = Collections.singletonList(
                Arrays.asList("1", "Alice", "true")
        );
        return buildResultSet(columns, rows);
    }

    private ArrayResultSet buildMultiRowResultSet() {
        List<String> columns = Arrays.asList("id", "value");
        List<List<Object>> rows = Arrays.asList(
                Arrays.asList("1", "100"),
                Arrays.asList("2", "200"),
                Arrays.asList("3", "300")
        );
        return buildResultSet(columns, rows);
    }

    // -------------------------------------------------------------------------
    // Navigation: next()
    // -------------------------------------------------------------------------

    @Test
    void next_onEmptyResultSet_returnsFalse() throws SQLException {
        ArrayResultSet rs = buildResultSet(Collections.singletonList("col"), Collections.emptyList());

        assertFalse(rs.next(), "next() on empty result set should return false");
    }

    @Test
    void next_onSingleRow_returnsTrueThenFalse() throws SQLException {
        ArrayResultSet rs = buildSingleRowResultSet();

        assertTrue(rs.next(), "first next() should return true");
        assertFalse(rs.next(), "second next() should return false after last row");
    }

    @Test
    void next_onMultipleRows_returnsTrueForEachRow() throws SQLException {
        ArrayResultSet rs = buildMultiRowResultSet();

        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next(), "Should return false after all 3 rows");
    }

    @Test
    void next_afterClose_throwsSQLException() throws SQLException {
        ArrayResultSet rs = buildSingleRowResultSet();
        rs.close();

        assertThrows(SQLException.class, rs::next);
    }

    // -------------------------------------------------------------------------
    // getString()
    // -------------------------------------------------------------------------

    @Test
    void getString_byColumnIndex_returnsCorrectValue() throws SQLException {
        ArrayResultSet rs = buildSingleRowResultSet();
        rs.next();

        assertEquals("1", rs.getString(1));
        assertEquals("Alice", rs.getString(2));
        assertEquals("true", rs.getString(3));
    }

    @Test
    void getString_byColumnLabel_returnsCorrectValue() throws SQLException {
        ArrayResultSet rs = buildSingleRowResultSet();
        rs.next();

        assertEquals("1", rs.getString("id"));
        assertEquals("Alice", rs.getString("name"));
        assertEquals("true", rs.getString("active"));
    }

    @Test
    void getString_nullValue_returnsNull() throws SQLException {
        List<String> columns = Collections.singletonList("col");
        List<List<Object>> rows = Collections.singletonList(
                Collections.singletonList(null)
        );
        ArrayResultSet rs = buildResultSet(columns, rows);
        rs.next();

        assertNull(rs.getString(1));
    }

    @Test
    void getString_unknownColumnLabel_throwsSQLException() throws SQLException {
        ArrayResultSet rs = buildSingleRowResultSet();
        rs.next();

        assertThrows(SQLException.class, () -> rs.getString("nonexistent_column"));
    }

    @Test
    void getString_columnIndexOutOfBounds_throwsSQLException() throws SQLException {
        ArrayResultSet rs = buildSingleRowResultSet();
        rs.next();

        assertThrows(SQLException.class, () -> rs.getString(99));
    }

    // -------------------------------------------------------------------------
    // getInt() and getLong()
    // -------------------------------------------------------------------------

    @Test
    void getInt_numericStringValue_returnsInt() throws SQLException {
        List<String> columns = Collections.singletonList("num");
        List<List<Object>> rows = Collections.singletonList(
                Collections.singletonList("42")
        );
        ArrayResultSet rs = buildResultSet(columns, rows);
        rs.next();

        assertEquals(42, rs.getInt(1));
    }

    @Test
    void getInt_nativeIntegerValue_returnsInt() throws SQLException {
        List<String> columns = Collections.singletonList("num");
        List<List<Object>> rows = Collections.singletonList(
                Collections.singletonList(99)
        );
        ArrayResultSet rs = buildResultSet(columns, rows);
        rs.next();

        assertEquals(99, rs.getInt(1));
    }

    @Test
    void getLong_numericStringValue_returnsLong() throws SQLException {
        List<String> columns = Collections.singletonList("num");
        List<List<Object>> rows = Collections.singletonList(
                Collections.singletonList("1234567890123")
        );
        ArrayResultSet rs = buildResultSet(columns, rows);
        rs.next();

        assertEquals(1234567890123L, rs.getLong(1));
    }

    // -------------------------------------------------------------------------
    // getBoolean()
    // -------------------------------------------------------------------------

    @Test
    void getBoolean_trueStringValue_returnsTrue() throws SQLException {
        List<String> columns = Collections.singletonList("flag");
        List<List<Object>> rows = Collections.singletonList(
                Collections.singletonList("true")
        );
        ArrayResultSet rs = buildResultSet(columns, rows);
        rs.next();

        assertTrue(rs.getBoolean(1));
    }

    @Test
    void getBoolean_nativeBooleanValue_returnsValue() throws SQLException {
        List<String> columns = Collections.singletonList("flag");
        List<List<Object>> rows = Collections.singletonList(
                Collections.singletonList(Boolean.TRUE)
        );
        ArrayResultSet rs = buildResultSet(columns, rows);
        rs.next();

        assertTrue(rs.getBoolean(1));
    }

    // -------------------------------------------------------------------------
    // getObject()
    // -------------------------------------------------------------------------

    @Test
    void getObject_byColumnIndex_returnsRawValue() throws SQLException {
        ArrayResultSet rs = buildSingleRowResultSet();
        rs.next();

        assertEquals("1", rs.getObject(1));
        assertEquals("Alice", rs.getObject(2));
    }

    @Test
    void getObject_byColumnLabel_returnsRawValue() throws SQLException {
        ArrayResultSet rs = buildSingleRowResultSet();
        rs.next();

        assertEquals("1", rs.getObject("id"));
        assertEquals("Alice", rs.getObject("name"));
    }

    // -------------------------------------------------------------------------
    // wasNull()
    // -------------------------------------------------------------------------

    @Test
    void wasNull_afterReadingNonNullValue_returnsFalse() throws SQLException {
        ArrayResultSet rs = buildSingleRowResultSet();
        rs.next();

        rs.getString(1);
        assertFalse(rs.wasNull(), "wasNull() should be false after reading a non-null value");
    }

    @Test
    void wasNull_afterReadingNullValue_returnsTrue() throws SQLException {
        List<String> columns = Collections.singletonList("col");
        List<List<Object>> rows = Collections.singletonList(
                Collections.singletonList(null)
        );
        ArrayResultSet rs = buildResultSet(columns, rows);
        rs.next();

        rs.getString(1);
        assertTrue(rs.wasNull(), "wasNull() should be true after reading a null value");
    }

    // -------------------------------------------------------------------------
    // getMetaData()
    // -------------------------------------------------------------------------

    @Test
    void getMetaData_returnsCorrectColumnCount() throws SQLException {
        ArrayResultSet rs = buildSingleRowResultSet();

        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(3, meta.getColumnCount());
    }

    @Test
    void getMetaData_returnsCorrectColumnNames() throws SQLException {
        ArrayResultSet rs = buildSingleRowResultSet();

        ResultSetMetaData meta = rs.getMetaData();
        assertEquals("id", meta.getColumnName(1));
        assertEquals("name", meta.getColumnName(2));
        assertEquals("active", meta.getColumnName(3));
    }

    @Test
    void getMetaData_emptyResultSet_returnsZeroColumns() throws SQLException {
        ArrayResultSet rs = buildResultSet(Collections.emptyList(), Collections.emptyList());

        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(0, meta.getColumnCount());
    }

    // -------------------------------------------------------------------------
    // close() and isClosed()
    // -------------------------------------------------------------------------

    @Test
    void isClosed_beforeClose_returnsFalse() throws SQLException {
        ArrayResultSet rs = buildSingleRowResultSet();

        assertFalse(rs.isClosed());
    }

    @Test
    void isClosed_afterClose_returnsTrue() throws SQLException {
        ArrayResultSet rs = buildSingleRowResultSet();
        rs.close();

        assertTrue(rs.isClosed());
    }

    @Test
    void close_calledMultipleTimes_doesNotThrow() throws SQLException {
        ArrayResultSet rs = buildSingleRowResultSet();
        rs.close();

        // Second close should be a no-op per JDBC spec
        assertDoesNotThrow(rs::close);
    }

    // -------------------------------------------------------------------------
    // Empty result set edge cases
    // -------------------------------------------------------------------------

    @Test
    void emptyResultSet_next_returnsFalse() throws SQLException {
        ArrayResultSet rs = buildResultSet(Arrays.asList("a", "b"), Collections.emptyList());

        assertFalse(rs.next());
    }

    @Test
    void emptyResultSet_columnCountIsCorrect() throws SQLException {
        List<String> columns = Arrays.asList("col1", "col2", "col3");
        ArrayResultSet rs = buildResultSet(columns, Collections.emptyList());

        assertEquals(3, rs.getMetaData().getColumnCount());
    }
}
