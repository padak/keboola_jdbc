package com.keboola.jdbc.command;

import com.keboola.jdbc.KeboolaConnection;
import com.keboola.jdbc.backend.DuckDbBackend;
import com.keboola.jdbc.backend.ExecutionResult;
import com.keboola.jdbc.config.DriverConfig;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Handles the {@code KEBOOLA SESSION LOG} command. Queries the DuckDB session log table
 * and returns all recorded SQL execution events ordered by most recent first.
 */
public class SessionLogHandler implements KeboolaCommandHandler {

    private static final Pattern SESSION_LOG_PATTERN = Pattern.compile(
            "^\\s*KEBOOLA\\s+SESSION\\s+LOG\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );

    @Override
    public boolean canHandle(String sql) {
        if (sql == null) return false;
        return SESSION_LOG_PATTERN.matcher(sql).matches();
    }

    @Override
    public ResultSet execute(String sql, KeboolaConnection connection) throws SQLException {
        DuckDbBackend duckDb = connection.getDuckDbBackend();
        if (duckDb == null) {
            throw new SQLException("Session log requires DuckDB backend");
        }

        String query = "SELECT * FROM " + DriverConfig.SESSION_LOG_TABLE + " ORDER BY executed_at DESC";
        ExecutionResult result = duckDb.execute(List.of(query));

        if (result.hasResultSet()) {
            return result.getResultSet();
        }
        throw new SQLException("Session log query did not return results");
    }
}
