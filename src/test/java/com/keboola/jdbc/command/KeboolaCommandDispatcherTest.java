package com.keboola.jdbc.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.keboola.jdbc.KeboolaConnection;
import com.keboola.jdbc.http.StorageApiClient;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for KeboolaCommandDispatcher - verifies routing to correct handlers.
 */
class KeboolaCommandDispatcherTest {

    private final KeboolaCommandDispatcher dispatcher = new KeboolaCommandDispatcher();

    // -------------------------------------------------------------------------
    // KEBOOLA HELP dispatching
    // -------------------------------------------------------------------------

    @Test
    void tryHandle_keboolaHelp_returnsNonNullResultSet() throws SQLException {
        ResultSet rs = dispatcher.tryHandle("KEBOOLA HELP", null);

        assertNotNull(rs, "KEBOOLA HELP should return a non-null ResultSet");
    }

    @Test
    void tryHandle_keboolaHelpLowerCase_returnsNonNullResultSet() throws SQLException {
        ResultSet rs = dispatcher.tryHandle("keboola help", null);

        assertNotNull(rs, "keboola help (lower case) should return a non-null ResultSet");
    }

    // -------------------------------------------------------------------------
    // Regular SQL - not intercepted
    // -------------------------------------------------------------------------

    @Test
    void tryHandle_regularSql_returnsNull() throws SQLException {
        ResultSet rs = dispatcher.tryHandle("SELECT 1", mock(KeboolaConnection.class));

        assertNull(rs, "Regular SQL should not be intercepted, expecting null");
    }

    @Test
    void tryHandle_insertStatement_returnsNull() throws SQLException {
        ResultSet rs = dispatcher.tryHandle("INSERT INTO t VALUES (1)", mock(KeboolaConnection.class));

        assertNull(rs, "INSERT statement should not be intercepted");
    }

    // -------------------------------------------------------------------------
    // Virtual table dispatching
    // -------------------------------------------------------------------------

    @Test
    void tryHandle_virtualTableComponents_returnsNonNullResultSet() throws Exception {
        KeboolaConnection mockConnection = mock(KeboolaConnection.class);
        StorageApiClient mockStorage = mock(StorageApiClient.class);
        when(mockConnection.getStorageClient()).thenReturn(mockStorage);
        when(mockStorage.listComponents()).thenReturn(Collections.<JsonNode>emptyList());

        ResultSet rs = dispatcher.tryHandle("SELECT * FROM _keboola.components", mockConnection);

        assertNotNull(rs, "_keboola.components should return a non-null ResultSet");
        verify(mockStorage).listComponents();
    }

    @Test
    void tryHandle_virtualTableComponents_returnsEmptyResultSetWhenNoComponents() throws Exception {
        KeboolaConnection mockConnection = mock(KeboolaConnection.class);
        StorageApiClient mockStorage = mock(StorageApiClient.class);
        when(mockConnection.getStorageClient()).thenReturn(mockStorage);
        when(mockStorage.listComponents()).thenReturn(Collections.<JsonNode>emptyList());

        ResultSet rs = dispatcher.tryHandle("SELECT * FROM _keboola.components", mockConnection);

        assertNotNull(rs);
        assertFalse(rs.next(), "Empty component list should yield no rows");
    }
}
