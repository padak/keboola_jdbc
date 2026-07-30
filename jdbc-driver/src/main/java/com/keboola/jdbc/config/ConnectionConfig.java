package com.keboola.jdbc.config;

import com.keboola.jdbc.auth.AuthMode;
import com.keboola.jdbc.auth.PatAuthProvider;
import com.keboola.jdbc.exception.KeboolaJdbcException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Parsed connection parameters derived from a JDBC URL and connection properties.
 *
 * Expected JDBC URL format:
 *   jdbc:keboola://connection.keboola.com[?token=...&branch=...&workspace=...&schema=...]
 *
 * Supported properties (may be supplied via Properties or as URL query parameters;
 * Properties take precedence when both are present):
 *   token     (required) - Keboola Storage API token or Personal Access Token. May also be
 *                          supplied via the standard {@code password} property — useful for
 *                          clients such as Tableau whose generic JDBC connector has no UI for
 *                          custom properties but does pass the dialog "Password" field to the
 *                          driver as {@code password}. This keeps the secret out of the
 *                          JDBC URL (which Tableau stores as plaintext in published data
 *                          sources). An explicit {@code token} wins over {@code password}.
 *   auth      (optional) - authentication mode, {@code token} or {@code pat}. When absent it
 *                          is inferred from the credential prefix.
 *   project   (optional) - numeric project ID the connection operates on. Only meaningful for
 *                          {@code auth=pat}, where a Personal Access Token spans several
 *                          projects; absent means the caller discovers it. Ignored for a
 *                          Storage API token, which already carries its project.
 *   branch    (optional) - branch ID to execute queries against
 *   workspace (optional) - workspace ID to use for query execution
 *   schema    (optional) - default schema (bucket) to use for unqualified table references
 *
 * The ID-valued properties ({@code project}, {@code branch}, {@code workspace}) accept either a
 * bare number or the {@code "<id> (<name>)"} form the driver advertises as a dropdown choice.
 */
public class ConnectionConfig {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionConfig.class);

    /**
     * Matches a valid DNS hostname: labels of alphanumeric + hyphens, separated by dots.
     * Rejects raw IP addresses, localhost, and special characters to prevent SSRF.
     */
    private static final Pattern VALID_HOSTNAME = Pattern.compile(
            "^([a-zA-Z0-9]([a-zA-Z0-9\\-]*[a-zA-Z0-9])?\\.)+[a-zA-Z]{2,}$"
    );

    /**
     * Matches an ID-valued property: a bare number, or the {@code "<id> (<name>)"} form that
     * {@code KeboolaDriver.getPropertyInfo} offers as a dropdown choice. IDE clients such as
     * DBeaver and DataGrip may store the selected choice string verbatim, so the driver has to
     * read back what it offered. The trailing label is greedy and may itself contain parentheses.
     *
     * <p>A leading minus is accepted so that a negative value reaches the range check of the
     * property that cares about it, rather than being reported as a malformed number.
     */
    private static final Pattern ID_WITH_OPTIONAL_LABEL = Pattern.compile(
            "^(-?\\d+)(\\s*\\(.*\\))?$"
    );

    private static final Set<String> KNOWN_KEYS = new HashSet<>(Arrays.asList(
            DriverConfig.PROP_TOKEN,
            DriverConfig.PROP_PASSWORD,
            DriverConfig.PROP_AUTH,
            DriverConfig.PROP_PROJECT,
            DriverConfig.PROP_BRANCH,
            DriverConfig.PROP_WORKSPACE,
            DriverConfig.PROP_SCHEMA
    ));

    /** Comma-separated list of the values the {@code auth} property accepts, for error messages. */
    private static final String ACCEPTED_AUTH_VALUES = Stream.of(AuthMode.values())
            .map(AuthMode::propertyValue)
            .collect(Collectors.joining(", "));

    private final String   host;
    private final String   token;
    private final AuthMode authMode;
    private final Long     projectId;
    private final Long     branchId;
    private final Long     workspaceId;
    private final String   schema;

    private ConnectionConfig(String host, String token, AuthMode authMode, Long projectId,
                             Long branchId, Long workspaceId, String schema) {
        this.host        = host;
        this.token       = token;
        this.authMode    = authMode;
        this.projectId   = projectId;
        this.branchId    = branchId;
        this.workspaceId = workspaceId;
        this.schema      = schema;
    }

    /**
     * Parses the JDBC URL and connection properties into a validated {@link ConnectionConfig}.
     *
     * @param url   JDBC URL, e.g. "jdbc:keboola://connection.keboola.com"
     * @param props connection properties containing at minimum "token"
     * @return a fully validated {@link ConnectionConfig} instance
     * @throws KeboolaJdbcException if the URL is malformed, the host is empty, the token is
     *                              missing, or "auth"/"project" carry an unusable value
     */
    public static ConnectionConfig fromUrl(String url, Properties props) throws KeboolaJdbcException {
        if (url == null || !url.startsWith(DriverConfig.URL_PREFIX)) {
            throw KeboolaJdbcException.connectionFailed(
                    "Invalid JDBC URL. Expected format: " + DriverConfig.URL_PREFIX + "<host>"
            );
        }

        String host = extractHost(url);
        if (host.isEmpty()) {
            // NOTE: do not echo the URL — it may carry a token in its query string.
            throw KeboolaJdbcException.connectionFailed(
                    "Host must not be empty in JDBC URL. Expected format: "
                            + DriverConfig.URL_PREFIX + "<host>"
            );
        }

        if (!VALID_HOSTNAME.matcher(host).matches()) {
            throw KeboolaJdbcException.connectionFailed(
                    "Invalid host in JDBC URL: '" + host + "'. "
                    + "Only valid DNS hostnames are accepted (IP addresses and localhost are rejected)"
            );
        }

        // Merge URL query parameters into properties. Properties win over URL params.
        // Unknown keys are warned on for URL-sourced params only — IDE clients (DBeaver,
        // DataGrip, ...) routinely inject their own properties (user, password, ssl, etc.)
        // which would otherwise produce a warning on every connection.
        Properties effectiveProps = new Properties();
        Properties urlParams = new Properties();
        mergeQueryParams(urlParams, url);
        warnOnUnknownKeys(urlParams);
        effectiveProps.putAll(urlParams);
        if (props != null) {
            for (String name : props.stringPropertyNames()) {
                effectiveProps.setProperty(name, props.getProperty(name));
            }
        }

        // The credential may be supplied as 'token' or, as a fallback, via the standard
        // 'password' property. The fallback lets Tableau users keep the secret out of the
        // JDBC URL by typing it into the connection dialog's Password field. An explicit
        // 'token' always wins over 'password'.
        String token = firstNonBlank(
                effectiveProps.getProperty(DriverConfig.PROP_TOKEN),
                effectiveProps.getProperty(DriverConfig.PROP_PASSWORD)
        );
        if (token == null) {
            throw KeboolaJdbcException.authenticationFailed(
                    "Property 'token' is required but was not provided "
                            + "(supply a Storage API token or a Personal Access Token as 'token', "
                            + "or via the 'password' field)"
            );
        }
        token = token.trim();

        AuthMode authMode = resolveAuthMode(effectiveProps, token);
        Long projectId    = parseProjectId(effectiveProps);

        if (authMode == AuthMode.STORAGE_TOKEN && projectId != null) {
            LOG.warn("Property '{}' is ignored with '{}' authentication — a Storage API token "
                            + "is already scoped to a single project.",
                    DriverConfig.PROP_PROJECT, AuthMode.STORAGE_TOKEN.propertyValue());
            projectId = null;
        }

        Long branchId    = parseOptionalId(effectiveProps, DriverConfig.PROP_BRANCH);
        Long workspaceId = parseOptionalId(effectiveProps, DriverConfig.PROP_WORKSPACE);
        String schema    = parseOptionalString(effectiveProps, DriverConfig.PROP_SCHEMA);

        return new ConnectionConfig(host, token, authMode, projectId, branchId, workspaceId, schema);
    }

    /**
     * Determines how the credential is presented to the Keboola APIs. An explicit {@code auth}
     * property wins even when it contradicts the credential's prefix — the user may be holding a
     * credential whose shape the driver does not recognize.
     *
     * @throws KeboolaJdbcException if {@code auth} is present but names no known mode
     */
    private static AuthMode resolveAuthMode(Properties props, String credential) throws KeboolaJdbcException {
        String raw = parseOptionalString(props, DriverConfig.PROP_AUTH);
        if (raw == null) {
            return PatAuthProvider.looksLikePat(credential) ? AuthMode.PAT : AuthMode.STORAGE_TOKEN;
        }
        AuthMode mode = AuthMode.fromPropertyValue(raw);
        if (mode == null) {
            throw KeboolaJdbcException.connectionFailed(
                    "Property 'auth' has an unrecognized value '" + raw
                            + "'. Accepted values: " + ACCEPTED_AUTH_VALUES
            );
        }
        return mode;
    }

    /**
     * Reads the optional {@code project} property as a positive project ID.
     *
     * @return the project ID, or null when the property is absent
     * @throws KeboolaJdbcException if the value is not a positive number
     */
    private static Long parseProjectId(Properties props) throws KeboolaJdbcException {
        Long projectId = parseOptionalId(props, DriverConfig.PROP_PROJECT);
        if (projectId != null && projectId <= 0) {
            throw KeboolaJdbcException.connectionFailed(
                    "Property 'project' must be a positive project ID, got: " + projectId
            );
        }
        return projectId;
    }

    /**
     * Extracts the hostname from the JDBC URL by stripping the prefix and any trailing path.
     * E.g. "jdbc:keboola://connection.keboola.com" -> "connection.keboola.com"
     */
    private static String extractHost(String url) {
        // Strip the jdbc:keboola:// prefix
        String remainder = url.substring(DriverConfig.URL_PREFIX.length());
        // Remove any trailing path
        int slashIndex = remainder.indexOf('/');
        if (slashIndex >= 0) {
            remainder = remainder.substring(0, slashIndex);
        }
        // Remove any trailing query string
        int queryIndex = remainder.indexOf('?');
        if (queryIndex >= 0) {
            remainder = remainder.substring(0, queryIndex);
        }
        return remainder.trim();
    }

    /**
     * Parses any query string in the JDBC URL ({@code ?k=v&k2=v2}) and writes the pairs
     * into {@code target}. Values are URL-decoded. Pairs without an "=" are ignored.
     * Anything before the first "?" is the host portion and is skipped here.
     */
    private static void mergeQueryParams(Properties target, String url) {
        int queryIndex = url.indexOf('?');
        if (queryIndex < 0 || queryIndex == url.length() - 1) {
            return;
        }
        String query = url.substring(queryIndex + 1);
        for (String pair : query.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String key = urlDecode(pair.substring(0, eq));
            String value = urlDecode(pair.substring(eq + 1));
            if (!key.isEmpty()) {
                target.setProperty(key, value);
            }
        }
    }

    /**
     * Percent-decodes a JDBC URL query value. Unlike {@code application/x-www-form-urlencoded},
     * a literal {@code '+'} stays a {@code '+'} — Keboola Storage API tokens commonly contain
     * a plus sign and most users will paste it without URL-encoding it as {@code %2B}.
     */
    private static String urlDecode(String s) {
        try {
            // Pre-escape literal '+' so URLDecoder doesn't turn it into a space.
            String safe = s.replace("+", "%2B");
            return java.net.URLDecoder.decode(safe, java.nio.charset.StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            // Malformed escape — fall back to the raw value rather than failing the whole connection
            return s;
        }
    }

    private static void warnOnUnknownKeys(Properties props) {
        for (String name : props.stringPropertyNames()) {
            if (!KNOWN_KEYS.contains(name)) {
                LOG.warn("Unknown JDBC connection property '{}' — ignored. Known keys: {}",
                        name, KNOWN_KEYS);
            }
        }
    }

    /**
     * Returns the first argument that is neither null nor blank, or null if none qualify.
     */
    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                return value;
            }
        }
        return null;
    }

    /**
     * Reads an optional ID-valued property; returns null if absent or blank. Accepts a bare
     * number as well as the {@code "<id> (<name>)"} choice format, keeping every ID property
     * readable from the dropdown values the driver itself advertises.
     *
     * @throws KeboolaJdbcException if the value is present but carries no parsable ID
     */
    private static Long parseOptionalId(Properties props, String key) throws KeboolaJdbcException {
        String raw = props.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return null;
        }
        Matcher matcher = ID_WITH_OPTIONAL_LABEL.matcher(raw.trim());
        if (matcher.matches()) {
            try {
                return Long.parseLong(matcher.group(1));
            } catch (NumberFormatException e) {
                // Digits that overflow a long — reported as malformed like any other bad value.
            }
        }
        throw KeboolaJdbcException.connectionFailed(
                "Property '" + key + "' must be a valid number, got: " + raw
        );
    }

    /**
     * Reads an optional string property; returns null if absent or blank.
     */
    private static String parseOptionalString(Properties props, String key) {
        String raw = props.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return null;
        }
        return raw.trim();
    }

    // --- Getters ---

    /** Returns the Keboola Connection host, e.g. "connection.keboola.com". */
    public String getHost() {
        return host;
    }

    /**
     * Returns the credential used to authenticate requests — a Keboola Storage API token or a
     * Personal Access Token, depending on {@link #getAuthMode()}.
     */
    public String getToken() {
        return token;
    }

    /**
     * Returns how the credential is presented to the Keboola APIs; never null. Reflects the
     * explicit {@code auth} property when given, otherwise the credential's own prefix.
     */
    public AuthMode getAuthMode() {
        return authMode;
    }

    /**
     * Returns the project ID if explicitly configured for a Personal Access Token, or null to
     * indicate that the caller should discover it. Always null for a Storage API token, whose
     * project is fixed by the credential itself.
     */
    public Long getProjectId() {
        return projectId;
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

    /**
     * Returns the default schema (bucket name) if explicitly configured, or null.
     * When set, unqualified table references in SQL will be qualified with this schema.
     */
    public String getSchema() {
        return schema;
    }

    @Override
    public String toString() {
        return "ConnectionConfig{host='" + host + "', authMode=" + authMode
                + ", projectId=" + projectId + ", branchId=" + branchId
                + ", workspaceId=" + workspaceId + ", schema=" + schema + "}";
    }
}
