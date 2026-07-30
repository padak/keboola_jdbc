package com.keboola.jdbc.config;

import com.keboola.jdbc.auth.AuthMode;
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
    private static final String VALID_PAT = "kbc_pat_0123456789abcdef";

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
    void fromUrl_withSchemaProperty_readsSchema() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);
        props.setProperty("schema", "in.c-main");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals("in.c-main", config.getSchema());
    }

    @Test
    void fromUrl_withoutSchema_returnsNull() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertNull(config.getSchema());
    }

    @Test
    void fromUrl_withBlankSchema_returnsNull() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);
        props.setProperty("schema", "  ");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertNull(config.getSchema());
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
    // fromUrl() - token via 'password' fallback (Tableau-friendly)
    // -------------------------------------------------------------------------

    @Test
    void fromUrl_tokenViaPasswordProperty_isAccepted() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("password", VALID_TOKEN);

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(VALID_TOKEN, config.getToken());
    }

    @Test
    void fromUrl_tokenViaPasswordProperty_isStoredTrimmed() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("password", "  " + VALID_TOKEN + "  ");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(VALID_TOKEN, config.getToken());
    }

    @Test
    void fromUrl_explicitTokenWinsOverPassword() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", "the-real-token");
        props.setProperty("password", "ignored-password");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals("the-real-token", config.getToken());
    }

    @Test
    void fromUrl_blankTokenFallsBackToPassword() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", "   ");
        props.setProperty("password", VALID_TOKEN);

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(VALID_TOKEN, config.getToken());
    }

    @Test
    void fromUrl_passwordInQueryString_isAccepted() throws KeboolaJdbcException {
        String url = "jdbc:keboola://connection.keboola.com?password=" + VALID_TOKEN;

        ConnectionConfig config = ConnectionConfig.fromUrl(url, new Properties());

        assertEquals(VALID_TOKEN, config.getToken());
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
    // fromUrl() - SSRF protection (hostname validation)
    // -------------------------------------------------------------------------

    @Test
    void fromUrl_ipAddress_throwsException() {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl("jdbc:keboola://169.254.169.254", props)
        );
        assertTrue(ex.getMessage().contains("Invalid host"), "Should reject IP addresses");
    }

    @Test
    void fromUrl_localhost_throwsException() {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl("jdbc:keboola://localhost", props)
        );
    }

    @Test
    void fromUrl_ipv6_throwsException() {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl("jdbc:keboola://[::1]", props)
        );
    }

    @Test
    void fromUrl_hostWithPort_throwsException() {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl("jdbc:keboola://evil.com:8080", props)
        );
    }

    @Test
    void fromUrl_validKeboolaHost_accepted() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        ConnectionConfig config = ConnectionConfig.fromUrl("jdbc:keboola://connection.north-europe.azure.keboola.com", props);
        assertEquals("connection.north-europe.azure.keboola.com", config.getHost());
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

    // ---------------------------------------------------------------------
    // URL query string parsing — Tableau-style "all-in-URL" connections
    // ---------------------------------------------------------------------

    @Test
    void fromUrl_tokenInQueryString_isAccepted() throws KeboolaJdbcException {
        String url = "jdbc:keboola://connection.keboola.com?token=" + VALID_TOKEN;

        ConnectionConfig config = ConnectionConfig.fromUrl(url, new Properties());

        assertEquals("connection.keboola.com", config.getHost());
        assertEquals(VALID_TOKEN, config.getToken());
    }

    @Test
    void fromUrl_allParamsInQueryString_areParsed() throws KeboolaJdbcException {
        String url = "jdbc:keboola://connection.keboola.com"
                + "?token=" + VALID_TOKEN
                + "&branch=42"
                + "&workspace=99"
                + "&schema=my-bucket";

        ConnectionConfig config = ConnectionConfig.fromUrl(url, new Properties());

        assertEquals(VALID_TOKEN, config.getToken());
        assertEquals(42L, config.getBranchId());
        assertEquals(99L, config.getWorkspaceId());
        assertEquals("my-bucket", config.getSchema());
    }

    @Test
    void fromUrl_propertiesOverrideQueryString() throws KeboolaJdbcException {
        String url = "jdbc:keboola://connection.keboola.com?token=url-token&branch=1";
        Properties props = new Properties();
        props.setProperty("token", "props-token");
        props.setProperty("branch", "999");

        ConnectionConfig config = ConnectionConfig.fromUrl(url, props);

        assertEquals("props-token", config.getToken());
        assertEquals(999L, config.getBranchId());
    }

    @Test
    void fromUrl_urlEncodedTokenIsDecoded() throws KeboolaJdbcException {
        // %2B must decode to '+' — Keboola tokens commonly contain a plus sign.
        String url = "jdbc:keboola://connection.keboola.com?token=abc%2Bdef";

        ConnectionConfig config = ConnectionConfig.fromUrl(url, new Properties());

        assertEquals("abc+def", config.getToken());
    }

    @Test
    void fromUrl_literalPlusInTokenIsPreserved() throws KeboolaJdbcException {
        // Unlike form-urlencoded, a literal '+' in a JDBC URL value must NOT become a space.
        // Users will paste tokens containing '+' without URL-encoding them as %2B.
        String url = "jdbc:keboola://connection.keboola.com?token=abc+def";

        ConnectionConfig config = ConnectionConfig.fromUrl(url, new Properties());

        assertEquals("abc+def", config.getToken());
    }

    @Test
    void fromUrl_emptyHostWithTokenInQueryDoesNotLeakToken() {
        // A malformed URL where the host slot is empty but the query carries a token
        // must NOT echo the token in the exception message (it ends up in logs).
        String url = "jdbc:keboola://?token=super-secret-token";

        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl(url, new Properties())
        );
        assertFalse(ex.getMessage().contains("super-secret-token"),
                "Exception message must not leak the token from the URL");
    }

    @Test
    void fromUrl_emptyQueryStringIsIgnored() throws KeboolaJdbcException {
        // Trailing "?" with no params used to break host extraction; verify it's tolerated
        String url = "jdbc:keboola://connection.keboola.com?";
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        ConnectionConfig config = ConnectionConfig.fromUrl(url, props);

        assertEquals("connection.keboola.com", config.getHost());
    }

    // -------------------------------------------------------------------------
    // 'auth' property — explicit mode selection
    // -------------------------------------------------------------------------

    @Test
    void fromUrl_explicitAuthPat_selectsPatMode() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);
        props.setProperty("auth", "pat");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(AuthMode.PAT, config.getAuthMode());
    }

    @Test
    void fromUrl_explicitAuthToken_selectsStorageTokenMode() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);
        props.setProperty("auth", "token");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(AuthMode.STORAGE_TOKEN, config.getAuthMode());
    }

    @Test
    void fromUrl_explicitAuthTokenOverridesPatPrefix() throws KeboolaJdbcException {
        // An explicit 'auth' wins over the credential's shape — the user may know better.
        Properties props = new Properties();
        props.setProperty("token", VALID_PAT);
        props.setProperty("auth", "token");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(AuthMode.STORAGE_TOKEN, config.getAuthMode());
    }

    @Test
    void fromUrl_authValueIsCaseInsensitiveAndTrimmed() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);
        props.setProperty("auth", "  PaT  ");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(AuthMode.PAT, config.getAuthMode());
    }

    @Test
    void fromUrl_blankAuth_fallsBackToInference() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_PAT);
        props.setProperty("auth", "   ");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(AuthMode.PAT, config.getAuthMode());
    }

    @Test
    void fromUrl_unrecognizedAuth_throwsListingAcceptedValues() {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);
        props.setProperty("auth", "oauth");

        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl(VALID_URL, props)
        );
        assertTrue(ex.getMessage().contains("'oauth'"), "Message should quote the rejected value");
        assertTrue(ex.getMessage().contains("token"), "Message should list 'token' as accepted");
        assertTrue(ex.getMessage().contains("pat"), "Message should list 'pat' as accepted");
    }

    // -------------------------------------------------------------------------
    // 'auth' property — inference from the credential prefix
    // -------------------------------------------------------------------------

    @Test
    void fromUrl_patPrefixedCredential_infersPatMode() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_PAT);

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(AuthMode.PAT, config.getAuthMode());
    }

    @Test
    void fromUrl_plainCredential_infersStorageTokenMode() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(AuthMode.STORAGE_TOKEN, config.getAuthMode());
    }

    @Test
    void fromUrl_patViaPasswordProperty_infersPatMode() throws KeboolaJdbcException {
        // Tableau path: the credential arrives in the dialog's Password field.
        Properties props = new Properties();
        props.setProperty("password", "  " + VALID_PAT + "  ");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(AuthMode.PAT, config.getAuthMode());
        assertEquals(VALID_PAT, config.getToken());
    }

    // -------------------------------------------------------------------------
    // 'project' property
    // -------------------------------------------------------------------------

    @Test
    void fromUrl_withProjectProperty_readsProjectId() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_PAT);
        props.setProperty("project", "1234");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(1234L, config.getProjectId());
    }

    @Test
    void fromUrl_withoutProject_returnsNull() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_PAT);

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertNull(config.getProjectId(), "Absent 'project' must stay null so it can be discovered");
    }

    @Test
    void fromUrl_blankProject_returnsNull() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_PAT);
        props.setProperty("project", "   ");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertNull(config.getProjectId());
    }

    @Test
    void fromUrl_projectIdBeyondIntegerRange_isParsedAsLong() throws KeboolaJdbcException {
        long largeId = Integer.MAX_VALUE + 7L;
        Properties props = new Properties();
        props.setProperty("token", VALID_PAT);
        props.setProperty("project", String.valueOf(largeId));

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(largeId, config.getProjectId());
    }

    @Test
    void fromUrl_nonNumericProject_throwsException() {
        Properties props = new Properties();
        props.setProperty("token", VALID_PAT);
        props.setProperty("project", "my-project");

        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl(VALID_URL, props)
        );
        assertTrue(ex.getMessage().contains("project"), "Message should name the 'project' property");
    }

    @Test
    void fromUrl_zeroProject_throwsException() {
        Properties props = new Properties();
        props.setProperty("token", VALID_PAT);
        props.setProperty("project", "0");

        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl(VALID_URL, props)
        );
        assertTrue(ex.getMessage().contains("positive"), "Message should say the ID must be positive");
    }

    @Test
    void fromUrl_negativeProject_throwsException() {
        Properties props = new Properties();
        props.setProperty("token", VALID_PAT);
        props.setProperty("project", "-1");

        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl(VALID_URL, props)
        );
        assertTrue(ex.getMessage().contains("positive"), "Message should say the ID must be positive");
    }

    @Test
    void fromUrl_projectWithStorageToken_isIgnoredNotFatal() throws KeboolaJdbcException {
        // A Storage API token already carries its project, so 'project' is dropped with a warning.
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);
        props.setProperty("project", "1234");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);

        assertEquals(AuthMode.STORAGE_TOKEN, config.getAuthMode());
        assertNull(config.getProjectId(), "'project' must be ignored for a Storage API token");
    }

    @Test
    void fromUrl_invalidProjectWithStorageToken_stillThrows() {
        // Validation runs before the mode decides whether the value is used, so a malformed
        // value is reported rather than silently dropped.
        Properties props = new Properties();
        props.setProperty("token", VALID_TOKEN);
        props.setProperty("project", "0");

        assertThrows(
                KeboolaJdbcException.class,
                () -> ConnectionConfig.fromUrl(VALID_URL, props)
        );
    }

    // -------------------------------------------------------------------------
    // 'auth' / 'project' via the URL query string
    // -------------------------------------------------------------------------

    @Test
    void fromUrl_authAndProjectInQueryString_areParsed() throws KeboolaJdbcException {
        String url = "jdbc:keboola://connection.keboola.com"
                + "?token=" + VALID_PAT
                + "&auth=pat"
                + "&project=4321";

        ConnectionConfig config = ConnectionConfig.fromUrl(url, new Properties());

        assertEquals(AuthMode.PAT, config.getAuthMode());
        assertEquals(4321L, config.getProjectId());
    }

    @Test
    void fromUrl_propertiesAuthOverridesQueryStringAuth() throws KeboolaJdbcException {
        String url = "jdbc:keboola://connection.keboola.com?token=" + VALID_PAT + "&auth=pat";
        Properties props = new Properties();
        props.setProperty("auth", "token");

        ConnectionConfig config = ConnectionConfig.fromUrl(url, props);

        assertEquals(AuthMode.STORAGE_TOKEN, config.getAuthMode());
    }

    @Test
    void fromUrl_propertiesProjectOverridesQueryStringProject() throws KeboolaJdbcException {
        String url = "jdbc:keboola://connection.keboola.com?token=" + VALID_PAT + "&project=111";
        Properties props = new Properties();
        props.setProperty("project", "222");

        ConnectionConfig config = ConnectionConfig.fromUrl(url, props);

        assertEquals(222L, config.getProjectId());
    }

    // -------------------------------------------------------------------------
    // ID properties accept the "<id> (<name>)" dropdown choice format
    //
    // KeboolaDriver.getPropertyInfo advertises choices in that shape and IDE clients may write
    // the selected string back verbatim, so every ID property must read back what it offered.
    // -------------------------------------------------------------------------

    /** The three properties parsed as IDs; each must behave identically. */
    private static final String[] ID_PROPERTIES = {"project", "branch", "workspace"};

    /** Builds a config with {@code property} set to {@code value} and a PAT credential. */
    private static ConnectionConfig withIdProperty(String property, String value) throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_PAT);
        props.setProperty(property, value);
        return ConnectionConfig.fromUrl(VALID_URL, props);
    }

    /** Reads back whichever ID property {@code property} names. */
    private static Long readIdProperty(ConnectionConfig config, String property) {
        switch (property) {
            case "project":   return config.getProjectId();
            case "branch":    return config.getBranchId();
            case "workspace": return config.getWorkspaceId();
            default: throw new IllegalArgumentException("Unhandled ID property: " + property);
        }
    }

    private static void assertIdPropertyRejected(String property, String value) {
        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> withIdProperty(property, value),
                "Property '" + property + "' must reject: <" + value + ">"
        );
        assertTrue(ex.getMessage().contains(property),
                "Message for '" + property + "' should name the property, was: " + ex.getMessage());
    }

    @Test
    void idProperties_bareNumber_isParsed() throws KeboolaJdbcException {
        for (String property : ID_PROPERTIES) {
            assertEquals(1234L, readIdProperty(withIdProperty(property, "1234"), property),
                    "Property '" + property + "' should accept a bare number");
        }
    }

    @Test
    void idProperties_choiceFormat_isParsed() throws KeboolaJdbcException {
        for (String property : ID_PROPERTIES) {
            assertEquals(1234L, readIdProperty(withIdProperty(property, "1234 (My Project)"), property),
                    "Property '" + property + "' should accept the '<id> (<name>)' choice format");
        }
    }

    @Test
    void idProperties_choiceFormatWithNestedParens_isParsed() throws KeboolaJdbcException {
        for (String property : ID_PROPERTIES) {
            assertEquals(1234L,
                    readIdProperty(withIdProperty(property, "1234 (Name with spaces (and parens))"), property),
                    "Property '" + property + "' should accept a label containing parentheses");
        }
    }

    @Test
    void idProperties_choiceFormatWithoutSpaceBeforeLabel_isParsed() throws KeboolaJdbcException {
        for (String property : ID_PROPERTIES) {
            assertEquals(1234L, readIdProperty(withIdProperty(property, "1234(My Project)"), property),
                    "Property '" + property + "' should not require a space before the label");
        }
    }

    @Test
    void idProperties_choiceFormatIsTrimmed() throws KeboolaJdbcException {
        for (String property : ID_PROPERTIES) {
            assertEquals(1234L, readIdProperty(withIdProperty(property, "  1234 (My Project)  "), property),
                    "Property '" + property + "' should tolerate surrounding whitespace");
        }
    }

    @Test
    void idProperties_choiceFormatBeyondIntegerRange_isParsed() throws KeboolaJdbcException {
        long largeId = Integer.MAX_VALUE + 7L;
        for (String property : ID_PROPERTIES) {
            assertEquals(largeId,
                    readIdProperty(withIdProperty(property, largeId + " (Big One)"), property),
                    "Property '" + property + "' should keep full long range in the choice format");
        }
    }

    @Test
    void idProperties_nonNumericValue_isRejected() {
        for (String property : ID_PROPERTIES) {
            assertIdPropertyRejected(property, "abc");
        }
    }

    @Test
    void idProperties_labelWithoutId_isRejected() {
        for (String property : ID_PROPERTIES) {
            assertIdPropertyRejected(property, "(no id)");
        }
    }

    @Test
    void idProperties_digitsInterruptedByLetters_isRejected() {
        for (String property : ID_PROPERTIES) {
            assertIdPropertyRejected(property, "12x34");
        }
    }

    @Test
    void idProperties_labelWithoutParentheses_isRejected() {
        for (String property : ID_PROPERTIES) {
            assertIdPropertyRejected(property, "1234 My Project");
        }
    }

    @Test
    void idProperties_unterminatedLabel_isRejected() {
        for (String property : ID_PROPERTIES) {
            assertIdPropertyRejected(property, "1234 (My Project");
        }
    }

    @Test
    void idProperties_valueOverflowingLong_isRejected() {
        for (String property : ID_PROPERTIES) {
            assertIdPropertyRejected(property, "99999999999999999999");
        }
    }

    @Test
    void idProperties_blankValue_isTreatedAsAbsent() throws KeboolaJdbcException {
        // Blank means "absent" for every ID property. IDE clients routinely pass empty
        // properties, and failing the connection on one would break them.
        for (String property : ID_PROPERTIES) {
            assertNull(readIdProperty(withIdProperty(property, "   "), property),
                    "Blank '" + property + "' should be treated as absent");
            assertNull(readIdProperty(withIdProperty(property, ""), property),
                    "Empty '" + property + "' should be treated as absent");
        }
    }

    @Test
    void idProperties_rejectionMessageQuotesTheOffendingValue() {
        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> withIdProperty("branch", "12x34")
        );
        assertEquals("Connection failed: Property 'branch' must be a valid number, got: 12x34",
                ex.getMessage());
    }

    @Test
    void projectId_negativeInChoiceFormat_reportedAsNonPositive() {
        // The format check must let a negative through so the range check owns the message.
        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> withIdProperty("project", "-1 (Some Project)")
        );
        assertTrue(ex.getMessage().contains("positive"),
                "Expected the positivity message, was: " + ex.getMessage());
    }

    @Test
    void projectId_zeroInChoiceFormat_reportedAsNonPositive() {
        KeboolaJdbcException ex = assertThrows(
                KeboolaJdbcException.class,
                () -> withIdProperty("project", "0 (Some Project)")
        );
        assertTrue(ex.getMessage().contains("positive"),
                "Expected the positivity message, was: " + ex.getMessage());
    }

    @Test
    void branchAndWorkspace_negativeValue_isNotRangeChecked() throws KeboolaJdbcException {
        // Only 'project' constrains the range; branch/workspace keep accepting what they always did.
        assertEquals(-1L, readIdProperty(withIdProperty("branch", "-1"), "branch"));
        assertEquals(-1L, readIdProperty(withIdProperty("workspace", "-1"), "workspace"));
    }

    @Test
    void idProperties_choiceFormatInQueryString_isParsed() throws KeboolaJdbcException {
        // A choice string reaching the driver through the URL is percent-encoded by the client.
        String url = "jdbc:keboola://connection.keboola.com"
                + "?token=" + VALID_PAT
                + "&project=1234%20%28My%20Project%29"
                + "&branch=42%20%28main%29";

        ConnectionConfig config = ConnectionConfig.fromUrl(url, new Properties());

        assertEquals(1234L, config.getProjectId());
        assertEquals(42L, config.getBranchId());
    }

    // -------------------------------------------------------------------------
    // toString() with the auth fields
    // -------------------------------------------------------------------------

    @Test
    void toString_includesAuthModeAndProjectIdWithoutLeakingPat() throws KeboolaJdbcException {
        Properties props = new Properties();
        props.setProperty("token", VALID_PAT);
        props.setProperty("project", "1234");

        ConnectionConfig config = ConnectionConfig.fromUrl(VALID_URL, props);
        String str = config.toString();

        assertFalse(str.contains(VALID_PAT), "toString() must not expose the Personal Access Token");
        assertTrue(str.contains("PAT"), "toString() should report the auth mode");
        assertTrue(str.contains("1234"), "toString() should report the project ID");
    }
}
