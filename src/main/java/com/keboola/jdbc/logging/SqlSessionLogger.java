package com.keboola.jdbc.logging;

import com.keboola.jdbc.backend.DuckDbBackend;
import com.keboola.jdbc.config.DriverConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

/**
 * Logs every SQL execution event into a DuckDB system table for dialect compatibility analysis.
 * The log table is created lazily on the first log call.
 *
 * <p>Logging failures are silently swallowed - session logging should never break SQL execution.
 */
public class SqlSessionLogger {

    private static final Logger LOG = LoggerFactory.getLogger(SqlSessionLogger.class);

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS " + DriverConfig.SESSION_LOG_TABLE + " (" +
            "  executed_at TIMESTAMP DEFAULT current_timestamp, " +
            "  backend VARCHAR NOT NULL, " +
            "  sql_text VARCHAR NOT NULL, " +
            "  success BOOLEAN NOT NULL, " +
            "  error_message VARCHAR, " +
            "  duration_ms BIGINT, " +
            "  rows_affected BIGINT" +
            ")";

    private static final String INSERT_SQL =
            "INSERT INTO " + DriverConfig.SESSION_LOG_TABLE +
            " (executed_at, backend, sql_text, success, error_message, duration_ms, rows_affected) " +
            "VALUES (current_timestamp, ?, ?, ?, ?, ?, ?)";

    private final DuckDbBackend duckDb;
    private boolean initialized = false;

    /**
     * Creates a new session logger backed by the given DuckDB instance.
     *
     * @param duckDb the DuckDB backend to store log entries in
     */
    public SqlSessionLogger(DuckDbBackend duckDb) {
        this.duckDb = duckDb;
    }

    /**
     * Creates the log table if it does not exist yet. Thread-safe via synchronized.
     */
    private synchronized void ensureInitialized() throws SQLException {
        if (initialized) return;
        Connection conn = duckDb.getNativeConnection();
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(CREATE_TABLE_SQL);
        }
        initialized = true;
    }

    /**
     * Logs a SQL execution event. Failures are silently logged as warnings.
     *
     * @param backend      the backend type that executed the SQL (e.g. "queryservice", "duckdb")
     * @param sql          the SQL text that was executed
     * @param success      true if execution succeeded, false if it threw an exception
     * @param errorMessage the error message if execution failed, or null on success
     * @param durationMs   execution duration in milliseconds
     * @param rowsAffected number of rows affected (or -1 if unknown)
     */
    public void log(String backend, String sql, boolean success, String errorMessage,
                    long durationMs, long rowsAffected) {
        try {
            ensureInitialized();
            Connection conn = duckDb.getNativeConnection();
            try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
                ps.setString(1, backend);
                ps.setString(2, sql);
                ps.setBoolean(3, success);
                if (errorMessage != null) {
                    ps.setString(4, errorMessage);
                } else {
                    ps.setNull(4, Types.VARCHAR);
                }
                ps.setLong(5, durationMs);
                ps.setLong(6, rowsAffected);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            LOG.warn("Failed to log SQL execution: {}", e.getMessage());
        }
    }
}
