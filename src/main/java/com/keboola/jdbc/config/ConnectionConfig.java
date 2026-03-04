package com.keboola.jdbc.config;

import com.keboola.jdbc.exception.KeboolaJdbcException;

import java.util.Properties;

/**
 * Parsed connection parameters derived from a JDBC URL and connection properties.
 *
 * Expected JDBC URL format:
 *   jdbc:keboola://connection.keboola.com
 *
 * Supported properties:
 *   token     (required) - Keboola Storage API token
 *   branch    (optional) - branch ID to execute queries against
 *   workspace (optional) - workspace ID to use for query execution
 */
public class ConnectionConfig {

    private final String host;
    private final String token;
    private final Long   branchId;
    private final Long   workspaceId;

    private ConnectionConfig(String host, String token, Long branchId, Long workspaceId) {
        this.host        = host;
        this.token       = token;
        this.branchId    = branchId;
        this.workspaceId = workspaceId;
    }

    /**
     * Parses the JDBC URL and connection properties into a validated {@link ConnectionConfig}.
     *
     * @param url   JDBC URL, e.g. "jdbc:keboola://connection.keboola.com"
     * @param props connection properties containing at minimum "token"
     * @return a fully validated {@link ConnectionConfig} instance
     * @throws KeboolaJdbcException if the URL is malformed, the host is empty, or the token is missing
     */
    public static ConnectionConfig fromUrl(String url, Properties props) throws KeboolaJdbcException {
        if (url == null || !url.startsWith(DriverConfig.URL_PREFIX)) {
            throw KeboolaJdbcException.connectionFailed(
                    "Invalid JDBC URL. Expected format: " + DriverConfig.URL_PREFIX + "<host>"
            );
        }

        String host = extractHost(url);
        if (host.isEmpty()) {
            throw KeboolaJdbcException.connectionFailed(
                    "Host must not be empty in JDBC URL: " + url
            );
        }

        Properties effectiveProps = props != null ? props : new Properties();

        String token = effectiveProps.getProperty("token");
        if (token == null || token.trim().isEmpty()) {
            throw KeboolaJdbcException.authenticationFailed(
                    "Property 'token' is required but was not provided"
            );
        }

        Long branchId    = parseOptionalLong(effectiveProps, "branch");
        Long workspaceId = parseOptionalLong(effectiveProps, "workspace");

        return new ConnectionConfig(host, token.trim(), branchId, workspaceId);
    }

    /**
     * Extracts the hostname from the JDBC URL by stripping the prefix and any trailing path.
     * E.g. "jdbc:keboola://connection.keboola.com" -> "connection.keboola.com"
     */
    private static String extractHost(String url) {
        // Strip the jdbc:keboola:// prefix
        String remainder = url.substring(DriverConfig.URL_PREFIX.length());
        // Remove any trailing path or query string
        int slashIndex = remainder.indexOf('/');
        if (slashIndex >= 0) {
            remainder = remainder.substring(0, slashIndex);
        }
        return remainder.trim();
    }

    /**
     * Reads an optional long property; returns null if absent or blank.
     *
     * @throws KeboolaJdbcException if the value is present but not a valid number
     */
    private static Long parseOptionalLong(Properties props, String key) throws KeboolaJdbcException {
        String raw = props.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw KeboolaJdbcException.connectionFailed(
                    "Property '" + key + "' must be a valid number, got: " + raw
            );
        }
    }

    // --- Getters ---

    /** Returns the Keboola Connection host, e.g. "connection.keboola.com". */
    public String getHost() {
        return host;
    }

    /** Returns the Keboola Storage API token used to authenticate requests. */
    public String getToken() {
        return token;
    }

    /**
     * Returns the branch ID if explicitly configured, or null to indicate that
     * the caller should discover the default branch via the Storage API.
     */
    public Long getBranchId() {
        return branchId;
    }

    /**
     * Returns the workspace ID if explicitly configured, or null to indicate that
     * the caller should create or discover a suitable workspace.
     */
    public Long getWorkspaceId() {
        return workspaceId;
    }

    @Override
    public String toString() {
        return "ConnectionConfig{host='" + host + "', branchId=" + branchId + ", workspaceId=" + workspaceId + "}";
    }
}
