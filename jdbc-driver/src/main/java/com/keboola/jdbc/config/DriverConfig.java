package com.keboola.jdbc.config;

import java.io.InputStream;
import java.util.Properties;

/**
 * Static driver-wide constants and default configuration values.
 * All tunables are defined here to avoid hardcoded values elsewhere in the codebase.
 */
public final class DriverConfig {

    private DriverConfig() {
        // Utility class - do not instantiate
    }

    // --- Driver identity ---

    public static final String DRIVER_NAME    = "Keboola JDBC Driver";
    /**
     * Build version, filtered from {@code version.properties} at build time. Resolves to the
     * Maven project version under any Maven build (including {@code mvn test}, whose
     * process-resources phase filters the resource). Falls back to "dev" only when the class is
     * loaded from an unfiltered resource -- e.g. run directly from {@code src/} in an IDE.
     */
    public static final String DRIVER_VERSION = loadVersion();
    /** Advertised JDBC major/minor version; bump manually on a major/minor release. */
    public static final int    MAJOR_VERSION  = 2;
    public static final int    MINOR_VERSION  = 1;

    private static String loadVersion() {
        try (InputStream in = DriverConfig.class.getResourceAsStream("/version.properties")) {
            if (in != null) {
                Properties props = new Properties();
                props.load(in);
                String v = props.getProperty("driver.version");
                if (v != null && !v.isEmpty() && !v.startsWith("${")) {
                    return v;
                }
            }
        } catch (Exception ignored) {
            // fall through to default
        }
        return "dev";
    }

    // --- JDBC URL ---

    /** Prefix that all Keboola JDBC URLs must start with. */
    public static final String URL_PREFIX = "jdbc:keboola://";

    // --- Connection property names ---
    // Each may be supplied via a Properties entry or as a JDBC URL query parameter.

    /** Storage API token or Personal Access Token. */
    public static final String PROP_TOKEN     = "token";

    /** Standard JDBC password property, accepted as a fallback carrier for {@link #PROP_TOKEN}. */
    public static final String PROP_PASSWORD  = "password";

    /** Authentication mode; values are {@code AuthMode.propertyValue()}. */
    public static final String PROP_AUTH      = "auth";

    /** Numeric project ID a Personal Access Token operates on. */
    public static final String PROP_PROJECT   = "project";

    /** Branch ID to execute queries against. */
    public static final String PROP_BRANCH    = "branch";

    /** Workspace ID to execute queries in. */
    public static final String PROP_WORKSPACE = "workspace";

    /** Default schema for unqualified table references. */
    public static final String PROP_SCHEMA    = "schema";

    // --- API paths ---

    /**
     * Lists the calling Personal Access Token and its descendants, each with the resolved set
     * of projects it can access. Authenticated by the bearer credential alone — being a
     * {@code /v1/auth/*} route it must not carry a project header. Returns 404 when
     * programmatic authentication is not enabled on the stack.
     */
    public static final String PAT_LIST_PATH = "/v1/auth/pat";

    // --- Result paging ---

    /** Default number of rows requested per page when fetching query results. Minimum allowed by API is 100. */
    public static final int DEFAULT_PAGE_SIZE = 1000;

    // --- Polling / backoff ---

    /** Initial delay before the first poll for job status, in milliseconds. */
    public static final long   POLL_INITIAL_INTERVAL_MS = 100;

    /** Maximum delay between consecutive polls, in milliseconds. */
    public static final long   POLL_MAX_INTERVAL_MS     = 2_000;

    /** Multiplicative factor applied to the poll interval after each unsuccessful poll. */
    public static final double POLL_BACKOFF_FACTOR       = 1.5;

    // --- Timeouts and retry ---

    /** Maximum total time to wait for a query job to reach a terminal state, in seconds. */
    public static final long MAX_WAIT_TIME_SECONDS = 300;

    /** Per-request HTTP connection/read/write timeout, in seconds. */
    public static final long HTTP_TIMEOUT_SECONDS  = 30;

    /** Number of times to retry a failed HTTP request before propagating the error. */
    public static final int  MAX_RETRIES           = 3;

    // --- Virtual tables ---

    /** Default row limit for virtual table queries when no LIMIT is specified. */
    public static final int VIRTUAL_TABLE_DEFAULT_LIMIT = 100;

    // --- Filtered databases ---

    /** Snowflake system databases that should be hidden from the sidebar. */
    public static final java.util.Set<String> FILTERED_DATABASES = java.util.Collections.unmodifiableSet(
            new java.util.HashSet<>(java.util.Arrays.asList("SNOWFLAKE", "SNOWFLAKE_LEARNING_DB", "SNOWFLAKE_SAMPLE_DATA"))
    );

}
