package com.keboola.jdbc;

import com.keboola.jdbc.command.KeboolaCommandDispatcher;
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
import java.util.ArrayList;
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

    /** ResultSet from a custom command (KEBOOLA HELP, virtual tables). Mutually exclusive with currentResultSet. */
    protected ResultSet currentCustomResultSet;

    /** Dispatcher for custom Keboola commands (HELP, virtual tables). */
    private final KeboolaCommandDispatcher commandDispatcher = new KeboolaCommandDispatcher();

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

        // Intercept USE SCHEMA/DATABASE commands locally (also sent to server via session)
        interceptUseCommand(sql);

        // Intercept custom Keboola commands (HELP, virtual tables) before sending to Query Service
        ResultSet customResult = commandDispatcher.tryHandle(sql, connection);
        if (customResult != null) {
            currentResultSet = null;
            currentCustomResultSet = customResult;
            return true;
        }

        // Split input into individual statements (supports semicolon-separated SQL)
        List<String> statements = new ArrayList<>(splitStatements(sql));

        if (statements.isEmpty()) {
            updateCount = 0;
            return false;
        }

        try {
            // On first job (no session yet), prepend USE SCHEMA if set from connection property
            if (connection.getSessionId() == null) {
                String schema = connection.getSchema();
                if (schema != null && !schema.isEmpty()) {
                    String useSchema = "USE SCHEMA \"" + schema + "\"";
                    LOG.debug("First job — prepending schema from connection property: {}", useSchema);
                    statements.add(0, useSchema);
                }
            }

            // Submit job with session ID — the server-side session preserves
            // SET variables, USE SCHEMA, temp tables across jobs
            QueryJob job = queryClient.submitJob(branchId, workspaceId, statements, connection.getSessionId());
            currentJobId = job.getQueryJobId();

            JobStatus jobStatus = queryClient.waitForCompletion(job.getQueryJobId());

            List<StatementStatus> stmtStatuses = jobStatus.getStatements();
            if (stmtStatuses == null || stmtStatuses.isEmpty()) {
                throw new KeboolaJdbcException("Job " + job.getQueryJobId() + " returned no statement results");
            }

            // Check all statements for errors
            for (StatementStatus s : stmtStatuses) {
                processStatementStatus(s);
            }

            // The last statement determines the result type
            StatementStatus lastStatus = stmtStatuses.get(stmtStatuses.size() - 1);
            String lastSql = statements.get(statements.size() - 1).trim().toUpperCase();

            Integer numRows = lastStatus.getNumberOfRows();
            boolean isSelect = numRows != null && numRows > 0;

            if (!isSelect) {
                isSelect = lastSql.startsWith("SELECT")
                        || lastSql.startsWith("SHOW")
                        || lastSql.startsWith("DESCRIBE")
                        || lastSql.startsWith("WITH");
            }

            if (isSelect) {
                currentResultSet = fetchFirstPage(job.getQueryJobId(), lastStatus.getId());
                return true;
            } else {
                Integer affected = lastStatus.getRowsAffected();
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
        return currentCustomResultSet != null ? currentCustomResultSet : currentResultSet;
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

    // buildStatements removed — with server-side sessions, no preamble needed.
    // SET, USE SCHEMA, temp tables all persist in the session automatically.

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
        if (currentCustomResultSet != null) {
            return currentCustomResultSet;
        }
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
     * Splits a SQL string containing multiple semicolon-separated statements
     * into individual statements. Handles quoted strings to avoid splitting
     * on semicolons inside string literals.
     *
     * @param sql the raw SQL input (may contain multiple statements)
     * @return list of individual non-empty statements
     */
    static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            } else if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
            }

            if (c == ';' && !inSingleQuote && !inDoubleQuote) {
                String stmt = current.toString().trim();
                if (!stmt.isEmpty()) {
                    statements.add(stmt);
                }
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        // Last statement (no trailing semicolon)
        String last = current.toString().trim();
        if (!last.isEmpty()) {
            statements.add(last);
        }

        return statements;
    }

    /**
     * Detects USE SCHEMA/DATABASE commands and updates the local schema tracking
     * on the connection (for getSchema() API). The command is still sent to the
     * server via the session — this only keeps local state in sync.
     *
     * Updates the field directly (not via setSchema()) to avoid recursion:
     * setSchema() -> execute() -> interceptUseCommand() -> setSchema() -> ...
     */
    private void interceptUseCommand(String sql) {
        for (String stmt : splitStatements(sql)) {
            Matcher matcher = USE_PATTERN.matcher(stmt);
            if (matcher.matches()) {
                String name = matcher.group(1);
                LOG.debug("Detected USE command - updating local schema to: {}", name);
                connection.updateLocalSchema(name);
            }
        }
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
        if (currentCustomResultSet != null) {
            try {
                currentCustomResultSet.close();
            } catch (SQLException e) {
                LOG.warn("Error closing custom result set", e);
            }
            currentCustomResultSet = null;
        }
    }
}
