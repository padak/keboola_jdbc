package com.keboola.jdbc.backend;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Abstraction for SQL execution backends.
 * Implementations include the Keboola Query Service (async HTTP API) and embedded DuckDB (local JDBC).
 */
public interface QueryBackend {

    /**
     * Executes one or more SQL statements and returns the result of the last one.
     *
     * @param statements list of SQL statements to execute sequentially
     * @return the execution result (ResultSet for queries, update count for DML)
     * @throws SQLException if execution fails
     */
    ExecutionResult execute(List<String> statements) throws SQLException;

    /**
     * Cancels the currently running query, if any.
     *
     * @throws SQLException if cancellation fails
     */
    void cancel() throws SQLException;

    /**
     * Returns the current catalog (database) from the backend.
     *
     * @return the current catalog name, or null if unknown
     * @throws SQLException if the query fails
     */
    String getCurrentCatalog() throws SQLException;

    /**
     * Returns the current schema from the backend.
     *
     * @return the current schema name, or null if unknown
     * @throws SQLException if the query fails
     */
    String getCurrentSchema() throws SQLException;

    /**
     * Sets the active catalog (database) on the backend.
     *
     * @param catalog the catalog name to switch to
     * @throws SQLException if the switch fails
     */
    void setCatalog(String catalog) throws SQLException;

    /**
     * Sets the active schema on the backend.
     *
     * @param schema the schema name to switch to
     * @throws SQLException if the switch fails
     */
    void setSchema(String schema) throws SQLException;

    /**
     * Returns native JDBC DatabaseMetaData if available.
     * DuckDB returns its real metadata; Query Service returns null (uses SHOW commands instead).
     *
     * @return native DatabaseMetaData or null
     * @throws SQLException if retrieval fails
     */
    DatabaseMetaData getNativeMetaData() throws SQLException;

    /**
     * Releases resources held by this backend.
     *
     * @throws SQLException if cleanup fails
     */
    void close() throws SQLException;

    /**
     * Returns the backend type identifier.
     *
     * @return "queryservice" or "duckdb"
     */
    String getBackendType();
}
