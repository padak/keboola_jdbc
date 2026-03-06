package com.keboola.jdbc.config;

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
    public static final String DRIVER_VERSION = "3.0.2-experimental";
    public static final int    MAJOR_VERSION  = 3;
    public static final int    MINOR_VERSION  = 0;

    // --- Backend types ---

    /** Backend type for the Keboola Query Service (async HTTP API). */
    public static final String BACKEND_QUERY_SERVICE = "queryservice";

    /** Backend type for embedded DuckDB (local JDBC). */
    public static final String BACKEND_DUCKDB = "duckdb";

    /** Default backend when not specified in connection properties. */
    public static final String DEFAULT_BACKEND = BACKEND_QUERY_SERVICE;

    // --- JDBC URL ---

    /** Prefix that all Keboola JDBC URLs must start with. */
    public static final String URL_PREFIX = "jdbc:keboola://";

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

    // --- Push command ---

    /** Number of rows per INSERT statement when pushing data to Snowflake via Query Service. */
    public static final int PUSH_BATCH_SIZE = 100;

    // --- Session logging ---

    /** System table name for SQL session logging (stored in DuckDB). */
    public static final String SESSION_LOG_TABLE = "_keboola_session_log";

    // --- Filtered databases ---

    // --- Kai AI assistant ---

    /** HTTP timeout for Kai API calls, in seconds. AI responses are slow, so this is much higher than regular HTTP. */
    public static final long   KAI_HTTP_TIMEOUT_SECONDS = 120;

    /** Service ID used to discover Kai's URL from the Storage API services index. */
    public static final String KAI_SERVICE_ID = "kai-assistant";

    /** Default chat model type for Kai conversations. API accepts "chat-model" or "chat-model-reasoning". */
    public static final String KAI_DEFAULT_MODEL = "chat-model";

    /** Visibility type for Kai chat sessions. */
    public static final String KAI_VISIBILITY_TYPE = "private";

    // --- Filtered databases ---

    /** Snowflake system databases that should be hidden from the sidebar. */
    public static final java.util.Set<String> FILTERED_DATABASES = java.util.Collections.unmodifiableSet(
            new java.util.HashSet<>(java.util.Arrays.asList("SNOWFLAKE", "SNOWFLAKE_LEARNING_DB"))
    );

}
