package com.keboola.jdbc;

import com.keboola.jdbc.auth.AuthMode;
import com.keboola.jdbc.auth.AuthProvider;
import com.keboola.jdbc.auth.PatAuthProvider;
import com.keboola.jdbc.auth.StorageTokenAuthProvider;
import com.keboola.jdbc.config.ConnectionConfig;
import com.keboola.jdbc.config.DriverConfig;
import com.keboola.jdbc.http.ProgrammaticAuthClient;
import com.keboola.jdbc.http.StorageApiClient;
import com.keboola.jdbc.http.model.Branch;
import com.keboola.jdbc.http.model.PatInfo;
import com.keboola.jdbc.http.model.Workspace;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC Driver entry point for Keboola Connection.
 *
 * <p>Connection URL format:
 * <pre>
 *   jdbc:keboola://&lt;host&gt;[?branch=&lt;id&gt;&amp;workspace=&lt;id&gt;]
 * </pre>
 *
 * <p>Supported connection properties:
 * <ul>
 *   <li>{@code token} (required) - Keboola Storage API token or Personal Access Token;
 *       also accepted via the standard {@code password} property</li>
 *   <li>{@code auth} (optional) - {@code token} or {@code pat}; inferred from the credential
 *       when absent</li>
 *   <li>{@code project} (optional) - Keboola project ID; required for a Personal Access Token
 *       that can reach more than one project</li>
 *   <li>{@code branch} (optional) - branch ID or name; defaults to the project's default branch</li>
 *   <li>{@code workspace} (optional) - workspace ID to run queries in; the newest workspace is
 *       selected when absent</li>
 * </ul>
 *
 * <p>The driver registers itself with the {@link DriverManager} in a static initializer.
 */
public class KeboolaDriver implements Driver {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(KeboolaDriver.class);

    /** JDBC URL prefix accepted by this driver. */
    public static final String URL_PREFIX = "jdbc:keboola://";

    // Self-register with the JDBC DriverManager on class load
    static {
        try {
            DriverManager.registerDriver(new KeboolaDriver());
            LOG.info("KeboolaDriver registered with DriverManager");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register KeboolaDriver", e);
        }
    }

    // -------------------------------------------------------------------------
    // Driver interface
    // -------------------------------------------------------------------------

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            // Per JDBC spec, return null if this driver does not handle the URL
            return null;
        }
        LOG.debug("Connecting to URL: {}", sanitizeUrlForLog(url));
        ConnectionConfig config;
        try {
            config = ConnectionConfig.fromUrl(url, info);
        } catch (com.keboola.jdbc.exception.KeboolaJdbcException e) {
            throw new SQLException("Invalid connection configuration: " + e.getMessage(), e);
        }
        return new KeboolaConnection(config);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        Properties effectiveInfo = (info != null) ? info : new Properties();
        // Mirror ConnectionConfig: the credential may also be supplied via the standard
        // 'password' property, so honor it here too when populating API-backed choices.
        String token = effectiveInfo.getProperty(DriverConfig.PROP_TOKEN, "");
        if (token.isEmpty()) {
            token = effectiveInfo.getProperty(DriverConfig.PROP_PASSWORD, "");
        }

        String authValue = effectiveInfo.getProperty(DriverConfig.PROP_AUTH, "");
        AuthMode authMode = resolveAuthMode(authValue, token);

        List<DriverPropertyInfo> props = new ArrayList<>();

        // --- token property ---
        DriverPropertyInfo tokenProp = new DriverPropertyInfo(DriverConfig.PROP_TOKEN, token);
        tokenProp.required = true;
        tokenProp.description = "Keboola Storage API token or Personal Access Token "
                + "(may also be supplied in the Password field)";
        props.add(tokenProp);

        // --- auth property ---
        DriverPropertyInfo authProp = new DriverPropertyInfo(DriverConfig.PROP_AUTH, authValue);
        authProp.required = false;
        // Kept ASCII: the description is rendered by the host application's UI
        authProp.description = "Authentication mode. Default: inferred from the credential - "
                + "a Personal Access Token is recognized by its prefix, anything else is treated "
                + "as a Storage API token";
        authProp.choices = new String[] {
                AuthMode.STORAGE_TOKEN.propertyValue(),
                AuthMode.PAT.propertyValue()
        };
        props.add(authProp);

        // --- project property ---
        String projectValue = effectiveInfo.getProperty(DriverConfig.PROP_PROJECT, "");
        DriverPropertyInfo projectProp = new DriverPropertyInfo(DriverConfig.PROP_PROJECT, projectValue);
        projectProp.required = false;
        projectProp.description = "Keboola project ID. Required for a Personal Access Token that "
                + "can reach more than one project; ignored for a Storage API token, which already "
                + "carries its project";
        projectProp.choices = new String[0];
        props.add(projectProp);

        // --- branch property ---
        String branchValue = effectiveInfo.getProperty(DriverConfig.PROP_BRANCH, "");
        DriverPropertyInfo branchProp = new DriverPropertyInfo(DriverConfig.PROP_BRANCH, branchValue);
        branchProp.required = false;
        branchProp.description = "Branch name or ID (default: main branch)";
        branchProp.choices = new String[0];

        // --- workspace property ---
        String workspaceValue = effectiveInfo.getProperty(DriverConfig.PROP_WORKSPACE, "");
        DriverPropertyInfo workspaceProp = new DriverPropertyInfo(DriverConfig.PROP_WORKSPACE, workspaceValue);
        workspaceProp.required = false;
        workspaceProp.description = "Workspace ID (default: the newest workspace in the project)";
        workspaceProp.choices = new String[0];

        String host = extractHost(url);

        // A Personal Access Token spans projects, so offer the ones it can reach.
        if (authMode == AuthMode.PAT && !token.isEmpty()) {
            projectProp.choices = loadProjectChoices(host, token);
        }

        // Branch and workspace live inside a project. With a Personal Access Token that project
        // is only known once 'project' is filled in, so the lookup waits until then.
        AuthProvider authProvider = buildChoiceAuthProvider(authMode, token, projectValue);
        if (authProvider != null) {
            try {
                StorageApiClient apiClient = new StorageApiClient(host, authProvider);

                List<Branch> branches = apiClient.listBranches();
                branchProp.choices = branches.stream()
                        .map(b -> b.getId() + " (" + b.getName() + ")")
                        .toArray(String[]::new);

                List<Workspace> workspaces = apiClient.listWorkspaces();
                workspaceProp.choices = workspaces.stream()
                        .map(w -> String.valueOf(w.getId()) + " (" + w.getName() + ")")
                        .toArray(String[]::new);

                LOG.debug("Populated {} branch choices and {} workspace choices",
                        branchProp.choices.length, workspaceProp.choices.length);

            } catch (Exception e) {
                // Non-fatal: return properties without choices so the user can enter values manually
                LOG.warn("Could not load API choices for getPropertyInfo (bad credential or network error): {}",
                        e.getMessage());
            }
        }

        props.add(branchProp);
        props.add(workspaceProp);

        return props.toArray(new DriverPropertyInfo[0]);
    }

    @Override
    public int getMajorVersion() {
        return DriverConfig.MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return DriverConfig.MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        // Keboola JDBC driver is not fully JDBC-compliant (read-only, no transactions, etc.)
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("java.util.logging not used by this driver");
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Resolves the authentication mode the connection form is describing. An explicit
     * {@code auth} value wins; an unrecognized one falls back to detection from the credential
     * so that filling in the form stays possible (the error surfaces on connect).
     */
    private AuthMode resolveAuthMode(String authValue, String token) {
        AuthMode explicit = AuthMode.fromPropertyValue(authValue);
        if (explicit != null) {
            return explicit;
        }
        return PatAuthProvider.looksLikePat(token) ? AuthMode.PAT : AuthMode.STORAGE_TOKEN;
    }

    /**
     * Lists the projects a Personal Access Token can reach, formatted as {@code id (name)}.
     * Returns an empty array when the lookup fails, leaving the field free-text.
     */
    private String[] loadProjectChoices(String host, String pat) {
        try {
            List<PatInfo> pats = new ProgrammaticAuthClient(host, pat).listPersonalAccessTokens();
            Map<Long, String> projectNamesById = new LinkedHashMap<>();
            for (PatInfo patInfo : pats) {
                for (PatInfo.ProjectAccess project : patInfo.getAccessibleProjects()) {
                    projectNamesById.putIfAbsent(project.getId(), project.getName());
                }
            }
            LOG.debug("Populated {} project choices", projectNamesById.size());
            return projectNamesById.entrySet().stream()
                    .map(entry -> entry.getKey() + " (" + entry.getValue() + ")")
                    .toArray(String[]::new);
        } catch (Exception e) {
            // Non-fatal: the user can still type the project ID by hand
            LOG.warn("Could not load project choices for getPropertyInfo "
                    + "(bad credential, network error, or programmatic auth disabled): {}", e.getMessage());
            return new String[0];
        }
    }

    /**
     * Builds the provider used for the branch and workspace lookups, or null when the form does
     * not yet carry enough information for a Storage API call.
     */
    private AuthProvider buildChoiceAuthProvider(AuthMode authMode, String token, String projectValue) {
        if (token.isEmpty()) {
            return null;
        }
        if (authMode != AuthMode.PAT) {
            return new StorageTokenAuthProvider(token);
        }
        try {
            // Covers both an unparseable value and a non-positive project ID
            return new PatAuthProvider(token, Long.parseLong(projectValue.trim()));
        } catch (IllegalArgumentException e) {
            LOG.debug("Skipping branch/workspace choices: no usable '{}' value yet",
                    DriverConfig.PROP_PROJECT);
            return null;
        }
    }

    /**
     * Removes any token or sensitive query parameters from the URL before logging.
     */
    private String sanitizeUrlForLog(String url) {
        if (url == null) {
            return "null";
        }
        int queryStart = url.indexOf('?');
        if (queryStart >= 0) {
            return url.substring(0, queryStart) + "?<redacted>";
        }
        return url;
    }

    /**
     * Extracts the host part from a Keboola JDBC URL.
     *
     * <p>For {@code jdbc:keboola://connection.keboola.com?token=...}, returns
     * {@code connection.keboola.com}.
     *
     * @param url the JDBC URL
     * @return the host portion
     */
    private String extractHost(String url) {
        if (url == null || !url.startsWith(URL_PREFIX)) {
            return "";
        }
        // Remove prefix
        String remainder = url.substring(URL_PREFIX.length());
        // Strip query string if present
        int queryStart = remainder.indexOf('?');
        if (queryStart >= 0) {
            remainder = remainder.substring(0, queryStart);
        }
        // Strip trailing slash
        if (remainder.endsWith("/")) {
            remainder = remainder.substring(0, remainder.length() - 1);
        }
        return remainder;
    }
}
