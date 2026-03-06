package com.keboola.jdbc.command;

import com.keboola.jdbc.KeboolaConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;

class BackendSwitchHandlerTest {

    private BackendSwitchHandler handler;

    @BeforeEach
    void setUp() {
        handler = new BackendSwitchHandler();
    }

    @Test
    void canHandle_matchesDuckdb() {
        assertTrue(handler.canHandle("KEBOOLA USE BACKEND duckdb"));
    }

    @Test
    void canHandle_matchesQueryservice() {
        assertTrue(handler.canHandle("KEBOOLA USE BACKEND queryservice"));
    }

    @Test
    void canHandle_caseInsensitive() {
        assertTrue(handler.canHandle("keboola use backend DuckDB"));
    }

    @Test
    void canHandle_rejectsOtherSql() {
        assertFalse(handler.canHandle("SELECT 1"));
        assertFalse(handler.canHandle("USE SCHEMA foo"));
        assertFalse(handler.canHandle("KEBOOLA HELP"));
    }

    @Test
    void canHandle_rejectsNull() {
        assertFalse(handler.canHandle(null));
    }

    @Test
    void execute_switchesBackend() throws SQLException {
        KeboolaConnection connection = Mockito.mock(KeboolaConnection.class);

        ResultSet rs = handler.execute("KEBOOLA USE BACKEND duckdb", connection);

        verify(connection).switchBackend("duckdb");
        assertNotNull(rs);
        assertTrue(rs.next(), "ResultSet should have at least one row");
    }

    @Test
    void execute_returnsConfirmationResultSet() throws SQLException {
        KeboolaConnection connection = Mockito.mock(KeboolaConnection.class);

        ResultSet rs = handler.execute("KEBOOLA USE BACKEND duckdb", connection);

        assertTrue(rs.next());
        assertEquals("OK", rs.getString("STATUS"));
        assertEquals("duckdb", rs.getString("BACKEND"));
        assertNotNull(rs.getString("MESSAGE"));
        assertFalse(rs.next(), "ResultSet should have exactly one row");
    }
}
