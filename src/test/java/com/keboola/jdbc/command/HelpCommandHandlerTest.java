package com.keboola.jdbc.command;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for HelpCommandHandler - verifies pattern matching and result content.
 */
class HelpCommandHandlerTest {

    private final HelpCommandHandler handler = new HelpCommandHandler();

    // -------------------------------------------------------------------------
    // canHandle - positive cases
    // -------------------------------------------------------------------------

    @Test
    void canHandle_upperCase_returnsTrue() {
        assertTrue(handler.canHandle("KEBOOLA HELP"));
    }

    @Test
    void canHandle_lowerCase_returnsTrue() {
        assertTrue(handler.canHandle("keboola help"));
    }

    @Test
    void canHandle_extraWhitespace_returnsTrue() {
        assertTrue(handler.canHandle("  KEBOOLA  HELP  "));
    }

    @Test
    void canHandle_withSemicolon_returnsTrue() {
        assertTrue(handler.canHandle("KEBOOLA HELP;"));
    }

    @Test
    void canHandle_mixedCase_returnsTrue() {
        assertTrue(handler.canHandle("Keboola Help"));
    }

    // -------------------------------------------------------------------------
    // canHandle - negative cases
    // -------------------------------------------------------------------------

    @Test
    void canHandle_keboolaRun_returnsFalse() {
        assertFalse(handler.canHandle("KEBOOLA RUN"));
    }

    @Test
    void canHandle_selectStatement_returnsFalse() {
        assertFalse(handler.canHandle("SELECT 1"));
    }

    @Test
    void canHandle_bareHelp_returnsFalse() {
        assertFalse(handler.canHandle("HELP"));
    }

    @Test
    void canHandle_helpWithExtra_returnsFalse() {
        assertFalse(handler.canHandle("KEBOOLA HELP something"));
    }

    // -------------------------------------------------------------------------
    // execute - result content
    // -------------------------------------------------------------------------

    @Test
    void execute_returnsResultSetWithCorrectColumns() throws SQLException {
        ResultSet rs = handler.execute("KEBOOLA HELP", null);

        assertNotNull(rs, "execute should return a non-null ResultSet");

        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(3, meta.getColumnCount());
        assertEquals("command", meta.getColumnName(1));
        assertEquals("syntax", meta.getColumnName(2));
        assertEquals("description", meta.getColumnName(3));
    }

    @Test
    void execute_returnsAtLeastFiveRows() throws SQLException {
        ResultSet rs = handler.execute("KEBOOLA HELP", null);

        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
            // Every row should have non-empty values in all columns
            assertNotNull(rs.getString("command"));
            assertNotNull(rs.getString("syntax"));
            assertNotNull(rs.getString("description"));
        }

        assertTrue(rowCount >= 5,
                "Help output should contain at least 5 rows, got " + rowCount);
    }
}
