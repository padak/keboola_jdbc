package com.keboola.jdbc.auth;

import com.keboola.jdbc.exception.KeboolaJdbcException;

import java.util.Map;

/**
 * Supplies the authentication headers attached to every Keboola API request.
 *
 * <p>Headers are resolved per request rather than captured once, so an implementation
 * backed by an expiring credential can renew it transparently. Implementations must be
 * safe for concurrent use — a single JDBC connection issues requests from
 * {@code KeboolaStatement}, {@code KeboolaResultSet} and metadata lookups.
 */
public interface AuthProvider {

    /**
     * Returns the headers identifying the caller, e.g. {@code X-StorageApi-Token} for a
     * Storage API token or {@code Authorization} plus {@code X-KBC-ProjectId} for a
     * Personal Access Token.
     *
     * @return header name to value; never null or empty
     * @throws KeboolaJdbcException if a credential cannot be obtained or renewed
     */
    Map<String, String> authHeaders() throws KeboolaJdbcException;

    /** Returns the authentication mode, used for logging and error messages. */
    AuthMode mode();
}
