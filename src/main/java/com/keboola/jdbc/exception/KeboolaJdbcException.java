package com.keboola.jdbc.exception;

import java.sql.SQLException;

/**
 * Exception class for Keboola JDBC driver errors.
 * Extends {@link SQLException} to integrate with the standard JDBC exception hierarchy.
 *
 * SQLSTATE codes used:
 * - 08001: Connection exception (unable to establish connection)
 * - 28000: Authentication failure (invalid token or credentials)
 * - 42S02: Table or view not found
 * - HY000: General error (catch-all for unclassified driver errors)
 */
public class KeboolaJdbcException extends SQLException {

    private static final String SQLSTATE_CONNECTION_FAILED  = "08001";
    private static final String SQLSTATE_AUTH_FAILED        = "28000";
    private static final String SQLSTATE_TABLE_NOT_FOUND    = "42S02";
    private static final String SQLSTATE_GENERAL_ERROR      = "HY000";

    /**
     * Creates a new exception with just a message, using the general SQLSTATE HY000.
     * Prefer the factory methods for well-known error conditions.
     *
     * @param message human-readable description of the error
     */
    public KeboolaJdbcException(String message) {
        super(message, SQLSTATE_GENERAL_ERROR);
    }

    /**
     * Creates a new exception with message, SQLState, and root cause.
     *
     * @param message   human-readable description of the error
     * @param sqlState  five-character SQLState code
     * @param cause     underlying exception that triggered this error, or null
     */
    public KeboolaJdbcException(String message, String sqlState, Throwable cause) {
        super(message, sqlState, cause);
    }

    /**
     * Creates a new exception with message and SQLState but no cause.
     *
     * @param message  human-readable description of the error
     * @param sqlState five-character SQLState code
     */
    public KeboolaJdbcException(String message, String sqlState) {
        super(message, sqlState);
    }

    // --- Factory methods ---

    /**
     * Creates an exception indicating that the provided token was rejected (HTTP 401).
     *
     * @param detail  additional context, e.g. the host that rejected the token
     * @return exception with SQLSTATE 28000
     */
    public static KeboolaJdbcException authenticationFailed(String detail) {
        return new KeboolaJdbcException(
                "Authentication failed: " + detail,
                SQLSTATE_AUTH_FAILED
        );
    }

    /**
     * Creates an exception indicating that a connection to the Keboola API could not be established.
     *
     * @param detail  additional context, e.g. hostname or URL that was unreachable
     * @param cause   underlying network/IO exception
     * @return exception with SQLSTATE 08001
     */
    public static KeboolaJdbcException connectionFailed(String detail, Throwable cause) {
        return new KeboolaJdbcException(
                "Connection failed: " + detail,
                SQLSTATE_CONNECTION_FAILED,
                cause
        );
    }

    /**
     * Creates an exception indicating that a connection to the Keboola API could not be established.
     *
     * @param detail  additional context, e.g. hostname or URL that was unreachable
     * @return exception with SQLSTATE 08001
     */
    public static KeboolaJdbcException connectionFailed(String detail) {
        return new KeboolaJdbcException(
                "Connection failed: " + detail,
                SQLSTATE_CONNECTION_FAILED
        );
    }

    /**
     * Creates an exception indicating that a submitted query job failed during execution.
     *
     * @param queryJobId  the ID of the failed job
     * @param errorDetail error message returned by the query service
     * @return exception with SQLSTATE HY000
     */
    public static KeboolaJdbcException queryFailed(String queryJobId, String errorDetail) {
        return new KeboolaJdbcException(
                "Query job " + queryJobId + " failed: " + errorDetail,
                SQLSTATE_GENERAL_ERROR
        );
    }

    /**
     * Creates an exception indicating that a query job exceeded the maximum wait time.
     *
     * @param queryJobId      the ID of the timed-out job
     * @param maxWaitSeconds  the configured maximum wait time in seconds
     * @return exception with SQLSTATE HY000
     */
    public static KeboolaJdbcException timeout(String queryJobId, long maxWaitSeconds) {
        return new KeboolaJdbcException(
                "Query job " + queryJobId + " did not complete within " + maxWaitSeconds + " seconds",
                SQLSTATE_GENERAL_ERROR
        );
    }

    /**
     * Creates an exception indicating that a referenced table was not found in Keboola Storage.
     *
     * @param tableId  the full Keboola table ID (e.g. "in.c-main.my_table")
     * @return exception with SQLSTATE 42S02
     */
    public static KeboolaJdbcException tableNotFound(String tableId) {
        return new KeboolaJdbcException(
                "Table not found: " + tableId,
                SQLSTATE_TABLE_NOT_FOUND
        );
    }
}
