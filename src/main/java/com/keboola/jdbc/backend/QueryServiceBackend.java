package com.keboola.jdbc.backend;

import com.keboola.jdbc.config.DriverConfig;
import com.keboola.jdbc.KeboolaResultSet;
import com.keboola.jdbc.exception.KeboolaJdbcException;
import com.keboola.jdbc.http.QueryServiceClient;
import com.keboola.jdbc.http.model.JobStatus;
import com.keboola.jdbc.http.model.QueryJob;
import com.keboola.jdbc.http.model.QueryResult;
import com.keboola.jdbc.http.model.StatementStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Backend implementation that executes SQL via the Keboola Query Service (async HTTP API).
 * The actual database engine behind the API (Snowflake, BigQuery, etc.) is transparent to the driver.
 */
public class QueryServiceBackend implements QueryBackend {

    private static final Logger LOG = LoggerFactory.getLogger(QueryServiceBackend.class);

    private final QueryServiceClient queryClient;
    private final long branchId;
    private final long workspaceId;
    private final String sessionId;

    /** The job ID of the most recently submitted query, used for cancellation. */
    private volatile String currentJobId;

    public QueryServiceBackend(QueryServiceClient queryClient, long branchId,
                               long workspaceId, String sessionId) {
        this.queryClient = queryClient;
        this.branchId = branchId;
        this.workspaceId = workspaceId;
        this.sessionId = sessionId;
    }

    @Override
    public ExecutionResult execute(List<String> statements) throws SQLException {
        if (statements.isEmpty()) {
            return ExecutionResult.forUpdateCount(0);
        }

        try {
            QueryJob job = queryClient.submitJob(branchId, workspaceId, statements, sessionId);
            currentJobId = job.getQueryJobId();

            JobStatus jobStatus = queryClient.waitForCompletion(job.getQueryJobId());

            List<StatementStatus> stmtStatuses = jobStatus.getStatements();
            if (stmtStatuses == null || stmtStatuses.isEmpty()) {
                throw new KeboolaJdbcException("Job " + job.getQueryJobId() + " returned no statement results");
            }

            // Check all statements for errors
            for (StatementStatus s : stmtStatuses) {
                if ("failed".equals(s.getStatus())) {
                    throw new KeboolaJdbcException("Query execution failed: " + s.getError());
                }
                if ("canceled".equals(s.getStatus())) {
                    throw new KeboolaJdbcException("Query was canceled");
                }
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
                QueryResult firstPage = queryClient.fetchResults(
                        job.getQueryJobId(), lastStatus.getId(), 0, DriverConfig.DEFAULT_PAGE_SIZE);
                KeboolaResultSet rs = new KeboolaResultSet(
                        queryClient, job.getQueryJobId(), lastStatus.getId(), firstPage);
                return ExecutionResult.forResultSet(rs);
            } else {
                Integer affected = lastStatus.getRowsAffected();
                return ExecutionResult.forUpdateCount(affected != null ? affected : 0);
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
    public void cancel() throws SQLException {
        String jobId = currentJobId;
        if (jobId != null) {
            LOG.debug("Canceling job: jobId={}", jobId);
            try {
                queryClient.cancelJob(jobId);
            } catch (Exception e) {
                throw new SQLException("Failed to cancel job " + jobId + ": " + e.getMessage(), e);
            }
        }
    }

    @Override
    public String getCurrentCatalog() throws SQLException {
        return executeScalar("SELECT CURRENT_DATABASE()");
    }

    @Override
    public String getCurrentSchema() throws SQLException {
        return executeScalar("SELECT CURRENT_SCHEMA()");
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        execute(List.of("USE DATABASE \"" + catalog.replace("\"", "\"\"") + "\""));
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        execute(List.of("USE SCHEMA \"" + schema.replace("\"", "\"\"") + "\""));
    }

    @Override
    public DatabaseMetaData getNativeMetaData() {
        return null;
    }

    @Override
    public void close() throws SQLException {
        // Query Service backend has no persistent resources to close
    }

    @Override
    public String getBackendType() {
        return DriverConfig.BACKEND_QUERY_SERVICE;
    }

    // -- Accessors for internal use --

    public QueryServiceClient getQueryClient() {
        return queryClient;
    }

    public long getBranchId() {
        return branchId;
    }

    public long getWorkspaceId() {
        return workspaceId;
    }

    public String getSessionId() {
        return sessionId;
    }

    /**
     * Executes a single-column, single-row query and returns the string value.
     */
    private String executeScalar(String sql) throws SQLException {
        ExecutionResult result = execute(List.of(sql));
        if (result.hasResultSet()) {
            try (var rs = result.getResultSet()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        }
        return null;
    }
}
