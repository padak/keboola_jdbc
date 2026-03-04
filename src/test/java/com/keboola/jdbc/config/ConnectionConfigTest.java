package com.keboola.jdbc.config;

import com.keboola.jdbc.exception.KeboolaJdbcException;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ConnectionConfig - verifies URL parsing and property validation.
 */
class ConnectionConfigTest {

    private static final String VALID_URL = "jdbc:keboola://connection.keboola.com";
    private static final String VALID_TOKEN = "my-secret-token";

    // -------------------------------------------------------------------------
    // fromUrl() - valid input
    // -------------------------------------------------------------------------

    @Test
    void fromUrl_validUrlWithToken_createsConfig() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertNotNull(config);
    }

    @Test
    void fromUrl_validUrl_extractsHostCorrectly() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals("connection.keboola.com", config.getHost());
    }

    @Test
    void fromUrl_tokenIsStoredTrimmed() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", "  " + VALID_TOKEN + "  ");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(VALID_TOKEN, config.getToken());
    }

    @Test
    void fromUrl_withBranchProperty_readsBranchId() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);
        props.setProperty("branch", "42");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(42, config.getBranchId());
    }

    @Test
    void fromUrl_withWorkspaceProperty_readsWorkspaceId() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);
        props.setProperty("workspace", "123");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(123, config.getWorkspaceId());
    }

    @Test
    void fromUrl_withBothBranchAndWorkspace_readsBothIds() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);
        props.setProperty("branch", "10");
        props.setProperty("workspace", "20");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(10, config.getBranchId());
        assertEquals(20, config.getWorkspaceId());
    }

    @Test
    void fromUrl_withoutBranchOrWorkspace_returnsNullsForOptionals() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertNull(config.getBranchId());
        assertNull(config.getWorkspaceId());
    }

    @Test
    void fromUrl_urlWithTrailingPath_extractsHostCorrectly() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        ConnectionConfig config = ConnectionConfig.fromUrl("jdbc:keboola://connection.keboola.com/some/path", props);

        assertEquals("connection.keboola.com", config.getHost());
    }

    @Test
    void fromUrl_nullProperties_throwsMissingTokenException() {
        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl(VALID_URL, null)
        );
        assertTrue(ex.getMessage().contains("token"), "Exception message should mention 'token'");
    }

    // -------------------------------------------------------------------------
    // fromUrl() - missing or empty token
    // -------------------------------------------------------------------------

    @Test
    void fromUrl_missingToken_throwsException() {
        Properties props = new Properties();
        // No token property set

        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl(VALID_URL, props)
        );
        assertTrue(ex.getMessage().contains("token"), "Exception message should mention 'token'");
    }

    @Test
    void fromUrl_emptyToken_throwsException() {
        Properties props = new Properties();
        props.setProperty("token", "");

        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl(VALID_URL, props)
        );
        assertTrue(ex.getMessage().contains("token"), "Exception message should mention 'token'");
    }

    @Test
    void fromUrl_blankToken_throwsException() {
        Properties props = new Properties();
        props.setProperty("token", "   ");

        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl(VALID_URL, props)
        );
        assertTrue(ex.getMessage().contains("token"), "Exception message should mention 'token'");
    }

    // -------------------------------------------------------------------------
    // fromUrl() - invalid URL
    // -------------------------------------------------------------------------

    @Test
    void fromUrl_nullUrl_throwsException() {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl(null, props)
        );
    }

    @Test
    void fromUrl_wrongUrlPrefix_throwsException() {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl("jdbc:mysql://localhost", props)
        );
    }

    @Test
    void fromUrl_emptyHost_throwsException() {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        // URL with empty host: "jdbc:keboola://"
        assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl("jdbc:keboola://", props)
        );
    }

    // -------------------------------------------------------------------------
    // toString()
    // -------------------------------------------------------------------------

    @Test
    void toString_doesNotExposeToken() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);
        String str = config.toString();

        assertFalse(str.contains(VALID_TOKEN), "toString() must not expose the API token");
        assertTrue(str.contains("connection.keboola.com"), "toString() should include the host");
    }
}
