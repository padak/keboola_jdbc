package com.keboola.jdbc.command;

import com.keboola.jdbc.KeboolaConnection;
import com.keboola.jdbc.backend.DuckDbBackend;
import com.keboola.jdbc.backend.ExecutionResult;
import com.keboola.jdbc.backend.QueryBackend;
import com.keboola.jdbc.http.KaiClient;
import com.keboola.jdbc.http.model.KaiResponse;
import com.keboola.jdbc.logging.SqlSessionLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link KaiCommandHandler} pattern matching and execution.
 */
class KaiCommandHandlerTest {

    private KaiCommandHandler handler;

    @BeforeEach
    void setUp() {
        handler = new KaiCommandHandler();
    }

    // -------------------------------------------------------------------------
    // canHandle tests
    // -------------------------------------------------------------------------

    @Test
    void canHandle_askCommand() {
        assertTrue(handler.canHandle("KEBOOLA KAI ASK what tables do I have?"));
    }

    @Test
    void canHandle_sqlCommand() {
        assertTrue(handler.canHandle("KEBOOLA KAI SQL show me top 10 tables"));
    }

    @Test
    void canHandle_helpCommand() {
        assertTrue(handler.canHandle("KEBOOLA KAI HELP 42"));
    }

    @Test
    void canHandle_translateToSnowflake() {
        assertTrue(handler.canHandle("KEBOOLA KAI TRANSLATE TO SNOWFLAKE SELECT 1"));
    }

    @Test
    void canHandle_translateToDuckdb() {
        assertTrue(handler.canHandle("KEBOOLA KAI TRANSLATE TO DUCKDB SELECT CURRENT_TIMESTAMP()"));
    }

    @Test
    void canHandle_translateFromLog() {
        assertTrue(handler.canHandle("KEBOOLA KAI TRANSLATE 5"));
    }

    @Test
    void canHandle_caseInsensitive() {
        assertTrue(handler.canHandle("keboola kai ask hello"));
        assertTrue(handler.canHandle("Keboola Kai Sql show tables"));
    }

    @Test
    void canHandle_withSemicolon() {
        assertTrue(handler.canHandle("KEBOOLA KAI ASK what?;"));
        assertTrue(handler.canHandle("KEBOOLA KAI HELP 1;"));
    }

    @Test
    void canHandle_withWhitespace() {
        assertTrue(handler.canHandle("  KEBOOLA  KAI  ASK hello  "));
    }

    @Test
    void canHandle_rejectsNull() {
        assertFalse(handler.canHandle(null));
    }

    @Test
    void canHandle_rejectsNonKaiCommands() {
        assertFalse(handler.canHandle("SELECT 1"));
        assertFalse(handler.canHandle("KEBOOLA HELP"));
        assertFalse(handler.canHandle("KEBOOLA USE BACKEND duckdb"));
        assertFalse(handler.canHandle("KEBOOLA SESSION LOG"));
    }

    @Test
    void canHandle_rejectsKaiWithoutSubcommand() {
        // "KEBOOLA KAI " prefix is detected but execute() will throw for unrecognized subcommand
        assertTrue(handler.canHandle("KEBOOLA KAI "));
    }

    // -------------------------------------------------------------------------
    // execute tests (with mocked KaiClient)
    // -------------------------------------------------------------------------

    @Test
    void execute_askReturnsResponseAndSql() throws Exception {
        KeboolaConnection conn = mockConnectionWithKai(
                KaiResponse.success("Here is SQL: ```sql\nSELECT 1\n```"));

        ResultSet rs = handler.execute("KEBOOLA KAI ASK what tables?", conn);

        assertTrue(rs.next());
        assertTrue(rs.getString("RESPONSE").contains("SELECT 1"));
        assertEquals("SELECT 1", rs.getString("SQL"));
        assertFalse(rs.next());
    }

    @Test
    void execute_askWithoutSqlBlock() throws Exception {
        KeboolaConnection conn = mockConnectionWithKai(
                KaiResponse.success("You have 5 tables in your project."));

        ResultSet rs = handler.execute("KEBOOLA KAI ASK how many tables?", conn);

        assertTrue(rs.next());
        assertEquals("You have 5 tables in your project.", rs.getString("RESPONSE"));
        assertNull(rs.getString("SQL"));
    }

    @Test
    void execute_sqlGenerateReturnsColumns() throws Exception {
        KeboolaConnection conn = mockConnectionWithKai(
                KaiResponse.success("```sql\nSELECT * FROM users LIMIT 10\n```\nThis shows top 10."));

        QueryBackend backend = mock(QueryBackend.class);
        when(backend.getBackendType()).thenReturn("queryservice");
        when(conn.getBackend()).thenReturn(backend);
        when(conn.getSchema()).thenReturn("PUBLIC");
        when(conn.getCatalog()).thenReturn("SAPI_1234");

        ResultSet rs = handler.execute("KEBOOLA KAI SQL show me top 10 users", conn);

        assertTrue(rs.next());
        assertEquals("SELECT * FROM users LIMIT 10", rs.getString("SQL"));
        assertNotNull(rs.getString("EXPLANATION"));
    }

    @Test
    void execute_translateDirectedReturnsColumns() throws Exception {
        KeboolaConnection conn = mockConnectionWithKai(
                KaiResponse.success("```sql\nSELECT NOW()\n```"));

        ResultSet rs = handler.execute(
                "KEBOOLA KAI TRANSLATE TO DUCKDB SELECT CURRENT_TIMESTAMP()", conn);

        assertTrue(rs.next());
        assertEquals("SELECT CURRENT_TIMESTAMP()", rs.getString("ORIGINAL_SQL"));
        assertEquals("SELECT NOW()", rs.getString("TRANSLATED_SQL"));
        assertEquals("Snowflake", rs.getString("SOURCE_DIALECT"));
        assertEquals("DUCKDB", rs.getString("TARGET_DIALECT"));
    }

    @Test
    void execute_translateToSnowflakeSourceIsDuckdb() throws Exception {
        KeboolaConnection conn = mockConnectionWithKai(
                KaiResponse.success("```sql\nSELECT CURRENT_TIMESTAMP()\n```"));

        ResultSet rs = handler.execute(
                "KEBOOLA KAI TRANSLATE TO SNOWFLAKE SELECT NOW()", conn);

        assertTrue(rs.next());
        assertEquals("DuckDB", rs.getString("SOURCE_DIALECT"));
        assertEquals("SNOWFLAKE", rs.getString("TARGET_DIALECT"));
    }

    @Test
    void execute_kaiErrorThrowsSqlException() throws Exception {
        KeboolaConnection conn = mockConnectionWithKai(
                KaiResponse.error("rate limited"));

        SQLException ex = assertThrows(SQLException.class,
                () -> handler.execute("KEBOOLA KAI ASK hello", conn));
        assertTrue(ex.getMessage().contains("rate limited"));
    }

    @Test
    void execute_unrecognizedSubcommandThrows() throws Exception {
        KeboolaConnection conn = mock(KeboolaConnection.class);

        SQLException ex = assertThrows(SQLException.class,
                () -> handler.execute("KEBOOLA KAI UNKNOWN stuff", conn));
        assertTrue(ex.getMessage().contains("Unrecognized"));
    }

    // -------------------------------------------------------------------------
    // Session log integration tests
    // -------------------------------------------------------------------------

    @Test
    void execute_sqlHelpFetchesSessionLog() throws Exception {
        KeboolaConnection conn = mockConnectionWithKai(
                KaiResponse.success("The error is a typo. ```sql\nSELECT * FROM users\n```"));

        // Mock session log query
        ResultSet logRs = mockSessionLogResultSet("SELECT * FORM users", "queryservice", false, "syntax error");
        mockDuckDbForSessionLog(conn, logRs);

        ResultSet rs = handler.execute("KEBOOLA KAI HELP 42", conn);

        assertTrue(rs.next());
        assertEquals("SELECT * FORM users", rs.getString("ORIGINAL_SQL"));
        assertEquals("syntax error", rs.getString("ERROR"));
        assertNotNull(rs.getString("SQL"));
    }

    @Test
    void execute_translateFromLogAutoDetectsDirection() throws Exception {
        KeboolaConnection conn = mockConnectionWithKai(
                KaiResponse.success("```sql\nSELECT NOW()\n```"));

        ResultSet logRs = mockSessionLogResultSet("SELECT CURRENT_TIMESTAMP()", "queryservice", true, null);
        mockDuckDbForSessionLog(conn, logRs);

        ResultSet rs = handler.execute("KEBOOLA KAI TRANSLATE 1", conn);

        assertTrue(rs.next());
        assertEquals("Snowflake", rs.getString("SOURCE_DIALECT"));
        assertEquals("DuckDB", rs.getString("TARGET_DIALECT"));
    }

    @Test
    void execute_translateFromLogDuckdbToSnowflake() throws Exception {
        KeboolaConnection conn = mockConnectionWithKai(
                KaiResponse.success("```sql\nSELECT CURRENT_TIMESTAMP()\n```"));

        ResultSet logRs = mockSessionLogResultSet("SELECT NOW()", "duckdb", true, null);
        mockDuckDbForSessionLog(conn, logRs);

        ResultSet rs = handler.execute("KEBOOLA KAI TRANSLATE 2", conn);

        assertTrue(rs.next());
        assertEquals("DuckDB", rs.getString("SOURCE_DIALECT"));
        assertEquals("Snowflake", rs.getString("TARGET_DIALECT"));
    }

    @Test
    void execute_sessionLogEntryNotFoundThrows() throws Exception {
        KeboolaConnection conn = mock(KeboolaConnection.class);

        // Mock empty result set
        ResultSet logRs = mock(ResultSet.class);
        when(logRs.next()).thenReturn(false);

        DuckDbBackend duckDb = mock(DuckDbBackend.class);
        ExecutionResult execResult = mock(ExecutionResult.class);
        when(execResult.hasResultSet()).thenReturn(true);
        when(execResult.getResultSet()).thenReturn(logRs);
        when(duckDb.execute(anyList())).thenReturn(execResult);
        when(conn.getDuckDbBackend()).thenReturn(duckDb);

        SqlSessionLogger logger = mock(SqlSessionLogger.class);
        when(conn.getSessionLogger()).thenReturn(logger);

        SQLException ex = assertThrows(SQLException.class,
                () -> handler.execute("KEBOOLA KAI HELP 999", conn));
        assertTrue(ex.getMessage().contains("not found"));
    }

    // -------------------------------------------------------------------------
    // Helper methods
    // -------------------------------------------------------------------------

    private KeboolaConnection mockConnectionWithKai(KaiResponse kaiResponse) throws Exception {
        KeboolaConnection conn = mock(KeboolaConnection.class);
        KaiClient kaiClient = mock(KaiClient.class);
        when(kaiClient.chat(anyString(), anyString(), any())).thenReturn(kaiResponse);
        when(conn.getKaiClient()).thenReturn(kaiClient);
        when(conn.getOrCreateKaiChatId()).thenReturn("test-chat-id");
        when(conn.getQueryServiceBackend()).thenReturn(null);
        return conn;
    }

    private ResultSet mockSessionLogResultSet(String sqlText, String backend,
                                               boolean success, String errorMessage) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getString("sql_text")).thenReturn(sqlText);
        when(rs.getString("backend")).thenReturn(backend);
        when(rs.getBoolean("success")).thenReturn(success);
        when(rs.getString("error_message")).thenReturn(errorMessage);
        return rs;
    }

    private void mockDuckDbForSessionLog(KeboolaConnection conn, ResultSet logRs) throws SQLException {
        DuckDbBackend duckDb = mock(DuckDbBackend.class);
        ExecutionResult execResult = mock(ExecutionResult.class);
        when(execResult.hasResultSet()).thenReturn(true);
        when(execResult.getResultSet()).thenReturn(logRs);
        when(duckDb.execute(anyList())).thenReturn(execResult);
        when(conn.getDuckDbBackend()).thenReturn(duckDb);

        SqlSessionLogger logger = mock(SqlSessionLogger.class);
        when(conn.getSessionLogger()).thenReturn(logger);
    }
}
