package com.keboola.jdbc.backend;

import com.keboola.jdbc.config.DriverConfig;
import com.keboola.jdbc.http.QueryServiceClient;
import com.keboola.jdbc.http.model.JobStatus;
import com.keboola.jdbc.http.model.QueryJob;
import com.keboola.jdbc.http.model.QueryResult;
import com.keboola.jdbc.http.model.ResultColumn;
import com.keboola.jdbc.http.model.StatementStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link QueryServiceBackend}.
 * All interactions with the Query Service HTTP API are mocked via {@link QueryServiceClient}.
 */
@ExtendWith(MockitoExtension.class)
class QueryServiceBackendTest {

    private static final long BRANCH_ID = 123L;
    private static final long WORKSPACE_ID = 456L;
    private static final String SESSION_ID = "test-session-id";
    private static final String JOB_ID = "job-abc-123";
    private static final String STATEMENT_ID = "stmt-001";

    @Mock
    private QueryServiceClient queryClient;

    private QueryServiceBackend backend;

    @BeforeEach
    void setUp() {
        backend = new QueryServiceBackend(queryClient, BRANCH_ID, WORKSPACE_ID, SESSION_ID);
    }

    @Test
    void executeSelectDelegatesToQueryClient() throws Exception {
        // Arrange: mock a successful SELECT returning 2 rows
        String sql = "SELECT id, name FROM users";
        QueryJob queryJob = new QueryJob(JOB_ID, SESSION_ID);

        StatementStatus stmtStatus = new StatementStatus(
                STATEMENT_ID, sql, "completed", null, 2, 0);

        JobStatus jobStatus = new JobStatus(JOB_ID, "completed", List.of(stmtStatus));

        List<ResultColumn> columns = List.of(
                new ResultColumn("id", "INTEGER", true, null),
                new ResultColumn("name", "VARCHAR", true, 256)
        );
        List<List<String>> data = List.of(
                List.of("1", "Alice"),
                List.of("2", "Bob")
        );
        QueryResult queryResult = new QueryResult("completed", columns, data, null, 2);

        when(queryClient.submitJob(eq(BRANCH_ID), eq(WORKSPACE_ID), eq(List.of(sql)), eq(SESSION_ID)))
                .thenReturn(queryJob);
        when(queryClient.waitForCompletion(JOB_ID)).thenReturn(jobStatus);
        when(queryClient.fetchResults(eq(JOB_ID), eq(STATEMENT_ID), eq(0), eq(DriverConfig.DEFAULT_PAGE_SIZE)))
                .thenReturn(queryResult);

        // Act
        ExecutionResult result = backend.execute(List.of(sql));

        // Assert
        assertTrue(result.hasResultSet(), "SELECT should produce a ResultSet");
        assertNotNull(result.getResultSet(), "ResultSet should not be null");

        verify(queryClient).submitJob(BRANCH_ID, WORKSPACE_ID, List.of(sql), SESSION_ID);
        verify(queryClient).waitForCompletion(JOB_ID);
        verify(queryClient).fetchResults(JOB_ID, STATEMENT_ID, 0, DriverConfig.DEFAULT_PAGE_SIZE);
    }

    @Test
    void executeDmlReturnsUpdateCount() throws Exception {
        // Arrange: mock an INSERT with numberOfRows=0 and rowsAffected=5
        String sql = "INSERT INTO users (name) VALUES ('Alice')";
        QueryJob queryJob = new QueryJob(JOB_ID, SESSION_ID);

        StatementStatus stmtStatus = new StatementStatus(
                STATEMENT_ID, sql, "completed", null, 0, 5);

        JobStatus jobStatus = new JobStatus(JOB_ID, "completed", List.of(stmtStatus));

        when(queryClient.submitJob(eq(BRANCH_ID), eq(WORKSPACE_ID), eq(List.of(sql)), eq(SESSION_ID)))
                .thenReturn(queryJob);
        when(queryClient.waitForCompletion(JOB_ID)).thenReturn(jobStatus);

        // Act
        ExecutionResult result = backend.execute(List.of(sql));

        // Assert
        assertFalse(result.hasResultSet(), "DML should not produce a ResultSet");
        assertEquals(5, result.getUpdateCount(), "Update count should match rowsAffected");

        // fetchResults should NOT be called for DML
        verify(queryClient, never()).fetchResults(anyString(), anyString(), anyInt(), anyInt());
    }

    @Test
    void cancelCallsCancelJob() throws Exception {
        // Arrange: submit a job so currentJobId gets set
        String sql = "SELECT 1";
        QueryJob queryJob = new QueryJob(JOB_ID, SESSION_ID);

        StatementStatus stmtStatus = new StatementStatus(
                STATEMENT_ID, sql, "completed", null, 1, 0);
        JobStatus jobStatus = new JobStatus(JOB_ID, "completed", List.of(stmtStatus));

        List<ResultColumn> columns = List.of(new ResultColumn("1", "INTEGER", true, null));
        QueryResult queryResult = new QueryResult("completed", columns, List.of(List.of("1")), null, 1);

        when(queryClient.submitJob(eq(BRANCH_ID), eq(WORKSPACE_ID), eq(List.of(sql)), eq(SESSION_ID)))
                .thenReturn(queryJob);
        when(queryClient.waitForCompletion(JOB_ID)).thenReturn(jobStatus);
        when(queryClient.fetchResults(eq(JOB_ID), eq(STATEMENT_ID), eq(0), eq(DriverConfig.DEFAULT_PAGE_SIZE)))
                .thenReturn(queryResult);

        // Execute to set currentJobId
        backend.execute(List.of(sql));

        // Act
        backend.cancel();

        // Assert
        verify(queryClient).cancelJob(JOB_ID);
    }

    @Test
    void executeFailedStatementThrowsSQLException() throws Exception {
        // Arrange: mock a statement with status="failed"
        String sql = "SELECT * FROM nonexistent_table";
        String errorMsg = "Object 'NONEXISTENT_TABLE' does not exist.";
        QueryJob queryJob = new QueryJob(JOB_ID, SESSION_ID);

        StatementStatus stmtStatus = new StatementStatus(
                STATEMENT_ID, sql, "failed", errorMsg, null, null);

        JobStatus jobStatus = new JobStatus(JOB_ID, "completed", List.of(stmtStatus));

        when(queryClient.submitJob(eq(BRANCH_ID), eq(WORKSPACE_ID), eq(List.of(sql)), eq(SESSION_ID)))
                .thenReturn(queryJob);
        when(queryClient.waitForCompletion(JOB_ID)).thenReturn(jobStatus);

        // Act & Assert
        SQLException ex = assertThrows(SQLException.class, () -> backend.execute(List.of(sql)));
        assertTrue(ex.getMessage().contains(errorMsg),
                "Exception message should contain the original error: " + ex.getMessage());

        // fetchResults should NOT be called when statement failed
        verify(queryClient, never()).fetchResults(anyString(), anyString(), anyInt(), anyInt());
    }

    @Test
    void getBackendTypeReturnsQueryService() {
        assertEquals(DriverConfig.BACKEND_QUERY_SERVICE, backend.getBackendType());
        assertEquals("queryservice", backend.getBackendType());
    }
}
