package com.keboola.jdbc.config;

import com.keboola.jdbc.exception.KeboolaJdbcException;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DuckDB-related configuration in ConnectionConfig.
 */
class ConnectionConfigDuckDbTest {

    private static final String VALID_URL = "jdbc:keboola://connection.keboola.com";
    private static final String LOCALHOST_URL = "jdbc:keboola://localhost";
    private static final String VALID_TOKEN = "my-secret-token";

    // -------------------------------------------------------------------------
    // DuckDB backend - token not required
    // -------------------------------------------------------------------------

    @Test
    void parseDuckDbWithoutToken_succeeds() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("backend", "duckdb");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertNotNull(config);
        assertEquals("duckdb", config.getBackend());
        assertFalse(config.hasToken());
    }

    // -------------------------------------------------------------------------
    // DuckDB backend - duckdbPath property
    // -------------------------------------------------------------------------

    @Test
    void parseDuckDbWithDuckDbPath() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("backend", "duckdb");
        props.setProperty("duckdbPath", "/tmp/test.duckdb");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals("/tmp/test.duckdb", config.getDuckDbPath());
    }

    // -------------------------------------------------------------------------
    // Query Service backend - token still required
    // -------------------------------------------------------------------------

    @Test
    void parseQueryServiceStillRequiresToken() {
        Properties props = new Properties();
        // No token, no backend (defaults to queryservice)

        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl(VALID_URL, props)
        );
        assertTrue(ex.getMessage().contains("token"), "Exception message should mention 'token'");
    }

    // -------------------------------------------------------------------------
    // Default backend is queryservice
    // -------------------------------------------------------------------------

    @Test
    void defaultBackendIsQueryService() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals("queryservice", config.getBackend());
    }

    // -------------------------------------------------------------------------
    // DuckDB backend with token provided
    // -------------------------------------------------------------------------

    @Test
    void parseDuckDbWithToken_succeeds() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("backend", "duckdb");
        props.setProperty("token", VALID_TOKEN);

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertNotNull(config);
        assertEquals("duckdb", config.getBackend());
        assertTrue(config.hasToken());
        assertEquals(VALID_TOKEN, config.getToken());
    }

    // -------------------------------------------------------------------------
    // Invalid backend value
    // -------------------------------------------------------------------------

    @Test
    void parseInvalidBackend_throwsException() {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);
        props.setProperty("backend", "invalid");

        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl(VALID_URL, props)
        );
        assertTrue(ex.getMessage().contains("Invalid backend"),
                "Exception message should mention 'Invalid backend'");
    }

    // -------------------------------------------------------------------------
    // isDuckDb() helper
    // -------------------------------------------------------------------------

    @Test
    void isDuckDb_returnsCorrectly() throws KeboolaJdbcException {
        Properties duckDbProps = new Properties();
        duckDbProps.setProperty("backend", "duckdb");
        ConnectionConfig duckDbConfig = ConnectionConfig.fromUrl(VALID_URL, duckDbProps);

        Properties qsProps = new Properties();
        qsProps.setProperty("token", VALID_TOKEN);
        qsProps.setProperty("backend", "queryservice");
        ConnectionConfig qsConfig = ConnectionConfig.fromUrl(VALID_URL, qsProps);

        assertTrue(duckDbConfig.isDuckDb());
        assertFalse(qsConfig.isDuckDb());
    }

    // -------------------------------------------------------------------------
    // Localhost hostname handling
    // -------------------------------------------------------------------------

    @Test
    void localhostAllowedForDuckDb() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("backend", "duckdb");

        ConnectionConfig config = ConnectionConfig.fromUrl(LOCALHOST_URL, props);

        assertNotNull(config);
        assertEquals("localhost", config.getHost());
    }

    @Test
    void localhostRejectedForQueryService() {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);
        props.setProperty("backend", "queryservice");

        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl(LOCALHOST_URL, props)
        );
        assertTrue(ex.getMessage().contains("Invalid host"),
                "Exception message should mention 'Invalid host'");
    }
}
