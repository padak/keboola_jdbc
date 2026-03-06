package com.keboola.jdbc.backend;

import java.sql.ResultSet;

/**
 * Value object representing the result of executing SQL via a {@link QueryBackend}.
 * Either holds a ResultSet (for queries) or an update count (for DML statements).
 */
public final class ExecutionResult {

    private final ResultSet resultSet;
    private final int updateCount;

    private ExecutionResult(ResultSet resultSet, int updateCount) {
        this.resultSet = resultSet;
        this.updateCount = updateCount;
    }

    /**
     * Creates a result for a query that returned rows.
     *
     * @param rs the result set from the query
     * @return an ExecutionResult wrapping the ResultSet
     */
    public static ExecutionResult forResultSet(ResultSet rs) {
        return new ExecutionResult(rs, -1);
    }

    /**
     * Creates a result for a DML statement that affected rows.
     *
     * @param count the number of rows affected
     * @return an ExecutionResult with the update count
     */
    public static ExecutionResult forUpdateCount(int count) {
        return new ExecutionResult(null, count);
    }

    /**
     * Returns true if this result contains a ResultSet (i.e., was a query).
     */
    public boolean hasResultSet() {
        return resultSet != null;
    }

    /**
     * Returns the ResultSet, or null if this was a DML statement.
     */
    public ResultSet getResultSet() {
        return resultSet;
    }

    /**
     * Returns the update count, or -1 if this was a query.
     */
    public int getUpdateCount() {
        return updateCount;
    }
}
