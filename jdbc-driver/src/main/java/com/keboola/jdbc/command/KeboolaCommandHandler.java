package com.keboola.jdbc.command;

import com.keboola.jdbc.KeboolaConnection;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Handler for custom Keboola commands intercepted before SQL execution.
 * Implementations detect specific SQL patterns (e.g. KEBOOLA HELP, _keboola.* queries)
 * and return in-memory ResultSets without hitting the Query Service.
 */
public interface KeboolaCommandHandler {

    /**
     * Tests whether this handler can process the given SQL.
     *
     * @param sql the raw SQL string from the user
     * @return true if this handler should handle the SQL
     */
    boolean canHandle(String sql);

    /**
     * Executes the command and returns a ResultSet with the results.
     *
     * @param sql        the raw SQL string
     * @param connection the parent Keboola connection (provides API clients)
     * @return a ResultSet containing the command results
     * @throws SQLException if execution fails
     */
    ResultSet execute(String sql, KeboolaConnection connection) throws SQLException;
}
