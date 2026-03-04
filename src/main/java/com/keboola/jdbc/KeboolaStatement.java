package com.keboola.jdbc;

import com.keboola.jdbc.exception.KeboolaJdbcException;
import com.keboola.jdbc.http.QueryServiceClient;
import com.keboola.jdbc.http.model.JobStatus;
import com.keboola.jdbc.http.model.QueryJob;
import com.keboola.jdbc.http.model.QueryResult;
import com.keboola.jdbc.http.model.StatementStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JDBC Statement implementation that submits SQL queries to the Keboola Query Service,
 * polls for completion, and exposes results as a {@link KeboolaResultSet}.
 *
 * <p>Execution flow:
 * <ol>
 *   <li>Submit the SQL as a new query job via {@link QueryServiceClient#submitJob}</li>
 *   <li>Poll {@link QueryServiceClient#waitForCompletion} until the job reaches a terminal state</li>
 *   <li>Check the statement status - throw {@link KeboolaJdbcException} if failed or canceled</li>
 *   <li>Fetch the first page of results and wrap in {@link KeboolaResultSet}</li>
 * </ol>
 */
public class KeboolaStatement implements Statement {

    private static final Logger LOG = LoggerFactory.getLogger(KeboolaStatement.class);

    protected final KeboolaConnection connection;
    protected final QueryServiceClient queryClient;
    protected final long branchId;
    protected final long workspaceId;

    /** The job ID of the most recently submitted query, used for cancellation. */
    protected String currentJobId;

    /** The current open result set, if the last execution returned rows. */
    protected KeboolaResultSet currentResultSet;

    /**
     * The update count for the last DML statement (-1 if a ResultSet was returned,
     * or if no statement has been executed yet).
     */
    protected int updateCount = -1;

    /** User-configurable max rows limit (stored but not enforced server-side). */
    private int maxRows = 0;

    /** Query timeout in seconds (stored, passed to API where supported). */
    private int queryTimeout = 0;

    /**
     * Pattern to match USE SCHEMA/DATABASE commands.
     * Captures the schema/database name with or without quotes.
     * Examples: USE SCHEMA "in.c-main", USE DATABASE mydb, USE "in.c-test"
     */
    private static final Pattern USE_PATTERN = Pattern.compile(
            "^\\s*USE\\s+(?:SCHEMA|DATABASE)?\\s*\"?([^\"\\s;]+)\"?\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );

    private boolean closed = false;

    /**
     * Creates a new statement for the given connection.
     *
     * @param connection   the parent connection
     * @param queryClient  HTTP client for the Keboola Query Service
     * @param branchId     the branch ID to execute queries against
     * @param workspaceId  the workspace ID to execute queries against
     */
    public KeboolaStatement(KeboolaConnection connection,
                            QueryServiceClient queryClient,
                            long branchId,
                            long workspaceId) {
        this.connection = connection;
        this.queryClient = queryClient;
        this.branchId = branchId;
        this.workspaceId = workspaceId;
    }

    // -------------------------------------------------------------------------
    // Core execution
    // -------------------------------------------------------------------------

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        closeCurrentResultSet();
        updateCount = -1;

        LOG.debug("Executing SQL: {}", sql);

        // Intercept USE SCHEMA/DATABASE commands locally (not supported by Query Service API)
        if (interceptUseCommand(sql)) {
            return false;
        }

        // When a default schema is set, qualify unqualified table references
        sql = qualifyWithCurrentSchema(sql);

        try {
            QueryJob job = submitJob(sql);
            StatementStatus status = waitForJobCompletion(job.getQueryJobId());
            processStatementStatus(status);

            // Determine if this is a SELECT (has rows to fetch) or DML (only rows affected)
            Integer numRows = status.getNumberOfRows();
            boolean isSelect = numRows != null && numRows > 0;

            if (!isSelect) {
                // Check if SQL starts with SELECT-like keywords (fallback heuristic)
                String trimmedSql = sql.trim().toUpperCase();
                isSelect = trimmedSql.startsWith("SELECT")
                        || trimmedSql.startsWith("SHOW")
                        || trimmedSql.startsWith("DESCRIBE")
                        || trimmedSql.startsWith("WITH");
            }

            if (isSelect) {
                // SELECT statement - fetch first page and wrap in ResultSet
                currentResultSet = fetchFirstPage(job.getQueryJobId(), status.getId());
                return true;
            } else {
                // DML statement - no ResultSet
                Integer affected = status.getRowsAffected();
                updateCount = affected != null ? affected : 0;
                currentResultSet = null;
                return false;
            }

        } catch (KeboolaJdbcException e) {
            throw new SQLException(e.getMessage(), e);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Unexpected error executing SQL: " + e.getMessage(), e);
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        boolean hasResultSet = execute(sql);
        if (!hasResultSet) {
            throw new SQLException("SQL did not produce a ResultSet: " + sql);
        }
        return currentResultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        boolean hasResultSet = execute(sql);
        if (hasResultSet) {
            // Caller asked for update count but got rows - return 0
            return 0;
        }
        return updateCount >= 0 ? updateCount : 0;
    }

    // -------------------------------------------------------------------------
    // Execution sub-steps
    // -------------------------------------------------------------------------

    /**
     * Submits the SQL to the Query Service and returns the job descriptor.
     *
     * @param sql the SQL statement to execute
     * @return the query job with job ID and session ID
     * @throws Exception if the submission fails
     */
    private QueryJob submitJob(String sql) throws Exception {
        QueryJob job = queryClient.submitJob(branchId, workspaceId, sql);
        currentJobId = job.getQueryJobId();
        LOG.debug("Submitted job: jobId={}", currentJobId);
        return job;
    }

    /**
     * Polls until the job reaches a terminal state and returns the first statement's status.
     *
     * @param jobId the query job ID
     * @return the terminal StatementStatus for the first statement in the job
     * @throws Exception if polling fails, times out, or has no statements
     */
    private StatementStatus waitForJobCompletion(String jobId) throws Exception {
        LOG.debug("Waiting for job completion: jobId={}", jobId);
        JobStatus jobStatus = queryClient.waitForCompletion(jobId);
        LOG.debug("Job completed: jobId={}, status={}", jobId, jobStatus.getStatus());

        java.util.List<StatementStatus> statements = jobStatus.getStatements();
        if (statements == null || statements.isEmpty()) {
            throw new KeboolaJdbcException("Job " + jobId + " returned no statement results");
        }
        return statements.get(0);
    }

    /**
     * Validates that the statement completed successfully.
     *
     * @param status the terminal statement status
     * @throws KeboolaJdbcException if the statement failed or was canceled
     */
    private void processStatementStatus(StatementStatus status) throws KeboolaJdbcException {
        if ("failed".equals(status.getStatus())) {
            throw new KeboolaJdbcException("Query execution failed: " + status.getError());
        }
        if ("canceled".equals(status.getStatus())) {
            throw new KeboolaJdbcException("Query was canceled");
        }
    }

    /**
     * Fetches the first page of results for the given statement and wraps it in a
     * {@link KeboolaResultSet}.
     *
     * @param jobId       the query job ID (used for cancel support)
     * @param statementId the statement ID used for paging
     * @return the first-page KeboolaResultSet
     * @throws Exception if fetching fails
     */
    private KeboolaResultSet fetchFirstPage(String jobId, String statementId) throws Exception {
        QueryResult firstPage = queryClient.fetchResults(jobId, statementId, 0, KeboolaResultSet.PAGE_SIZE);
        return new KeboolaResultSet(queryClient, jobId, statementId, firstPage);
    }

    // -------------------------------------------------------------------------
    // Result access
    // -------------------------------------------------------------------------

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        // Keboola Query Service returns one result set per statement
        closeCurrentResultSet();
        return false;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        closeCurrentResultSet();
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("getGeneratedKeys not supported");
    }

    // -------------------------------------------------------------------------
    // Cancellation
    // -------------------------------------------------------------------------

    @Override
    public void cancel() throws SQLException {
        checkClosed();
        if (currentJobId != null) {
            LOG.debug("Canceling job: jobId={}", currentJobId);
            try {
                queryClient.cancelJob(currentJobId);
            } catch (Exception e) {
                throw new SQLException("Failed to cancel job " + currentJobId + ": " + e.getMessage(), e);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closeCurrentResultSet();
            closed = true;
            LOG.debug("KeboolaStatement closed");
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    // -------------------------------------------------------------------------
    // Configuration properties
    // -------------------------------------------------------------------------

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException("maxRows must be >= 0, got " + max);
        }
        this.maxRows = max;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            throw new SQLException("queryTimeout must be >= 0, got " + seconds);
        }
        this.queryTimeout = seconds;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        // no-op: not enforced
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // no-op: not applicable
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // no-op
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCursorName not supported");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("Only FETCH_FORWARD is supported");
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return KeboolaResultSet.PAGE_SIZE;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // no-op: page size is fixed at PAGE_SIZE
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        // no-op
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        // no-op: not supported
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    // -------------------------------------------------------------------------
    // Connection reference
    // -------------------------------------------------------------------------

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    // -------------------------------------------------------------------------
    // Batch execution (basic support)
    // -------------------------------------------------------------------------

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("addBatch not supported on KeboolaStatement; use KeboolaPreparedStatement");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("clearBatch not supported on KeboolaStatement");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("executeBatch not supported on KeboolaStatement");
    }

    // -------------------------------------------------------------------------
    // Execute variants with auto-generated key / result set type hints
    // -------------------------------------------------------------------------

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    // -------------------------------------------------------------------------
    // Wrapper interface
    // -------------------------------------------------------------------------

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Intercepts USE SCHEMA/DATABASE commands and handles them locally by setting
     * the current schema on the connection. The Keboola Query Service API does not
     * support USE commands, so we emulate them client-side.
     *
     * @param sql the SQL to check
     * @return true if the command was intercepted and handled, false if it should be sent to the API
     */
    private boolean interceptUseCommand(String sql) throws SQLException {
        Matcher matcher = USE_PATTERN.matcher(sql);
        if (matcher.matches()) {
            String name = matcher.group(1);
            LOG.debug("Intercepted USE command - setting schema to: {}", name);
            connection.setSchema(name);
            updateCount = 0;
            currentResultSet = null;
            return true;
        }
        return false;
    }

    /**
     * When a current schema is set on the connection, qualifies unqualified table references
     * in the SQL with the schema name. This is needed because the Query Service API is stateless
     * and does not support USE SCHEMA.
     *
     * <p>Handles patterns like:
     * <ul>
     *   <li>{@code FROM "table"} → {@code FROM "schema"."table"}</li>
     *   <li>{@code FROM table} → {@code FROM "schema"."table"}</li>
     *   <li>{@code JOIN "table"} → {@code JOIN "schema"."table"}</li>
     *   <li>{@code INTO "table"} → {@code INTO "schema"."table"}</li>
     * </ul>
     *
     * Table references that already contain a dot (schema-qualified) are left unchanged.
     *
     * @param sql the original SQL
     * @return the SQL with schema-qualified table names, or the original SQL if no schema is set
     */
    private String qualifyWithCurrentSchema(String sql) throws SQLException {
        String schema = connection.getSchema();
        if (schema == null || schema.isEmpty()) {
            return sql;
        }

        // Pattern matches FROM/JOIN/INTO/UPDATE followed by a table reference
        // Group 1: keyword (FROM, JOIN, INTO, UPDATE)
        // Group 2: optional quote
        // Group 3: table name (no dots = unqualified)
        // Group 4: closing quote (if opened)
        Pattern tableRefPattern = Pattern.compile(
                "(\\bFROM|\\bJOIN|\\bINTO|\\bUPDATE)\\s+\"([^\"]+)\"|" +
                "(\\bFROM|\\bJOIN|\\bINTO|\\bUPDATE)\\s+([a-zA-Z_][a-zA-Z0-9_]*)",
                Pattern.CASE_INSENSITIVE
        );

        StringBuffer result = new StringBuffer();
        Matcher m = tableRefPattern.matcher(sql);
        while (m.find()) {
            if (m.group(1) != null) {
                // Quoted table: FROM "tableName"
                String keyword = m.group(1);
                String tableName = m.group(2);
                if (tableName.contains(".")) {
                    // Already qualified (e.g. "in.c-main"."table" or "schema.table")
                    m.appendReplacement(result, Matcher.quoteReplacement(m.group()));
                } else {
                    String qualified = keyword + " \"" + schema + "\".\"" + tableName + "\"";
                    LOG.debug("Qualifying table reference: {} -> {}", m.group(), qualified);
                    m.appendReplacement(result, Matcher.quoteReplacement(qualified));
                }
            } else {
                // Unquoted table: FROM tableName
                String keyword = m.group(3);
                String tableName = m.group(4);
                if (tableName.contains(".")) {
                    m.appendReplacement(result, Matcher.quoteReplacement(m.group()));
                } else {
                    String qualified = keyword + " \"" + schema + "\".\"" + tableName + "\"";
                    LOG.debug("Qualifying table reference: {} -> {}", m.group(), qualified);
                    m.appendReplacement(result, Matcher.quoteReplacement(qualified));
                }
            }
        }
        m.appendTail(result);

        String qualifiedSql = result.toString();
        if (!qualifiedSql.equals(sql)) {
            LOG.info("Schema-qualified SQL: {}", qualifiedSql);
        }
        return qualifiedSql;
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }

    private void closeCurrentResultSet() {
        if (currentResultSet != null) {
            try {
                currentResultSet.close();
            } catch (SQLException e) {
                LOG.warn("Error closing result set", e);
            }
            currentResultSet = null;
        }
    }
}
