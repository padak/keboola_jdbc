package com.keboola.jdbc.command;

import com.keboola.jdbc.ArrayResultSet;
import com.keboola.jdbc.KeboolaConnection;
import com.keboola.jdbc.backend.DuckDbBackend;
import com.keboola.jdbc.backend.ExecutionResult;
import com.keboola.jdbc.config.DriverConfig;
import com.keboola.jdbc.http.KaiClient;
import com.keboola.jdbc.http.model.KaiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles {@code KEBOOLA KAI} commands for interacting with the Kai AI assistant.
 *
 * <p>Supported commands (each subcommand keyword is mutually exclusive):
 * <ul>
 *   <li>{@code KEBOOLA KAI ASK <question>} - free-form question</li>
 *   <li>{@code KEBOOLA KAI SQL <description>} - generate SQL for current backend</li>
 *   <li>{@code KEBOOLA KAI HELP <id>} - help fix a failed query from session log</li>
 *   <li>{@code KEBOOLA KAI TRANSLATE TO SNOWFLAKE|DUCKDB <sql>} - translate SQL</li>
 *   <li>{@code KEBOOLA KAI TRANSLATE <id>} - translate session log query (auto-detect direction)</li>
 * </ul>
 */
public class KaiCommandHandler implements KeboolaCommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(KaiCommandHandler.class);

    // Each subcommand keyword (ASK, SQL, HELP, TRANSLATE) is mutually exclusive - no ordering dependency
    private static final Pattern HELP_PATTERN = Pattern.compile(
            "^\\s*KEBOOLA\\s+KAI\\s+HELP\\s+(\\d+)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern SQL_PATTERN = Pattern.compile(
            "^\\s*KEBOOLA\\s+KAI\\s+SQL\\s+(.+?)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern TRANSLATE_DIRECTION_PATTERN = Pattern.compile(
            "^\\s*KEBOOLA\\s+KAI\\s+TRANSLATE\\s+TO\\s+(SNOWFLAKE|DUCKDB)\\s+(.+?)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern TRANSLATE_LOG_PATTERN = Pattern.compile(
            "^\\s*KEBOOLA\\s+KAI\\s+TRANSLATE\\s+(\\d+)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern ASK_PATTERN = Pattern.compile(
            "^\\s*KEBOOLA\\s+KAI\\s+ASK\\s+(.+?)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );

    // General prefix check for canHandle - fast rejection for non-Kai SQL
    private static final Pattern KAI_PREFIX = Pattern.compile(
            "^\\s*KEBOOLA\\s+KAI\\s+",
            Pattern.CASE_INSENSITIVE
    );

    @Override
    public boolean canHandle(String sql) {
        if (sql == null) return false;
        return KAI_PREFIX.matcher(sql).find();
    }

    @Override
    public ResultSet execute(String sql, KeboolaConnection connection) throws SQLException {
        Matcher matcher;

        // 1. KEBOOLA KAI HELP <id>
        matcher = HELP_PATTERN.matcher(sql);
        if (matcher.matches()) {
            int logId = Integer.parseInt(matcher.group(1));
            return executeSqlHelp(logId, connection);
        }

        // 2. KEBOOLA KAI SQL <description>
        matcher = SQL_PATTERN.matcher(sql);
        if (matcher.matches()) {
            String description = matcher.group(1);
            return executeSqlGenerate(description, connection);
        }

        // 3. KEBOOLA KAI TRANSLATE TO SNOWFLAKE|DUCKDB <sql>
        matcher = TRANSLATE_DIRECTION_PATTERN.matcher(sql);
        if (matcher.matches()) {
            String targetDialect = matcher.group(1).toUpperCase();
            String sqlToTranslate = matcher.group(2);
            return executeTranslateDirected(sqlToTranslate, targetDialect, connection);
        }

        // 4. KEBOOLA KAI TRANSLATE <id>
        matcher = TRANSLATE_LOG_PATTERN.matcher(sql);
        if (matcher.matches()) {
            int logId = Integer.parseInt(matcher.group(1));
            return executeTranslateFromLog(logId, connection);
        }

        // 5. KEBOOLA KAI ASK <question> (catch-all)
        matcher = ASK_PATTERN.matcher(sql);
        if (matcher.matches()) {
            String question = matcher.group(1);
            return executeAsk(question, connection);
        }

        throw new SQLException("Unrecognized Kai command. Use KEBOOLA HELP to see available commands.");
    }

    // -------------------------------------------------------------------------
    // Command implementations
    // -------------------------------------------------------------------------

    private ResultSet executeAsk(String question, KeboolaConnection connection) throws SQLException {
        KaiResponse response = sendToKai(question, connection);

        List<String> columns = Arrays.asList("RESPONSE", "SQL");
        List<List<Object>> rows = new ArrayList<>();
        rows.add(Arrays.asList(
                response.getFullText(),
                response.getExtractedSql()
        ));
        return new ArrayResultSet(columns, rows);
    }

    private ResultSet executeSqlGenerate(String description, KeboolaConnection connection) throws SQLException {
        String backendType = connection.getBackend().getBackendType();
        String schema = connection.getSchema();
        String catalog = connection.getCatalog();

        String prompt = String.format(
                "Generate a SQL query for %s. Current schema: %s, database: %s. "
                + "Return the SQL in a ```sql code block. Description: %s",
                backendType, schema, catalog, description
        );

        KaiResponse response = sendToKai(prompt, connection);

        List<String> columns = Arrays.asList("SQL", "EXPLANATION");
        List<List<Object>> rows = new ArrayList<>();
        rows.add(Arrays.asList(
                response.getExtractedSql(),
                response.getFullText()
        ));
        return new ArrayResultSet(columns, rows);
    }

    private ResultSet executeSqlHelp(int logId, KeboolaConnection connection) throws SQLException {
        SessionLogEntry entry = fetchSessionLogEntry(logId, connection);

        String status = entry.success ? "produced unexpected results" : "failed with error: " + entry.errorMessage;
        String prompt = String.format(
                "Help me fix this %s SQL query that %s:\n\n```sql\n%s\n```\n\n"
                + "Provide the corrected SQL in a ```sql code block and explain what was wrong.",
                entry.backend, status, entry.sqlText
        );

        KaiResponse response = sendToKai(prompt, connection);

        List<String> columns = Arrays.asList("RESPONSE", "SQL", "ORIGINAL_SQL", "ERROR");
        List<List<Object>> rows = new ArrayList<>();
        rows.add(Arrays.asList(
                response.getFullText(),
                response.getExtractedSql(),
                entry.sqlText,
                entry.errorMessage
        ));
        return new ArrayResultSet(columns, rows);
    }

    private ResultSet executeTranslateDirected(String sqlToTranslate, String targetDialect,
                                                KeboolaConnection connection) throws SQLException {
        String sourceDialect = "SNOWFLAKE".equals(targetDialect) ? "DuckDB" : "Snowflake";

        String prompt = String.format(
                "Translate this %s SQL to %s. Return only the translated SQL in a ```sql code block:\n\n```sql\n%s\n```",
                sourceDialect, targetDialect, sqlToTranslate
        );

        KaiResponse response = sendToKai(prompt, connection);

        List<String> columns = Arrays.asList("ORIGINAL_SQL", "TRANSLATED_SQL", "SOURCE_DIALECT", "TARGET_DIALECT");
        List<List<Object>> rows = new ArrayList<>();
        rows.add(Arrays.asList(
                sqlToTranslate,
                response.getExtractedSql(),
                sourceDialect,
                targetDialect
        ));
        return new ArrayResultSet(columns, rows);
    }

    private ResultSet executeTranslateFromLog(int logId, KeboolaConnection connection) throws SQLException {
        SessionLogEntry entry = fetchSessionLogEntry(logId, connection);

        // Auto-detect direction: queryservice (Snowflake) -> DuckDB, duckdb -> Snowflake
        String sourceDialect;
        String targetDialect;
        if (DriverConfig.BACKEND_QUERY_SERVICE.equals(entry.backend)) {
            sourceDialect = "Snowflake";
            targetDialect = "DuckDB";
        } else {
            sourceDialect = "DuckDB";
            targetDialect = "Snowflake";
        }

        String prompt = String.format(
                "Translate this %s SQL to %s. Return only the translated SQL in a ```sql code block:\n\n```sql\n%s\n```",
                sourceDialect, targetDialect, entry.sqlText
        );

        KaiResponse response = sendToKai(prompt, connection);

        List<String> columns = Arrays.asList("ORIGINAL_SQL", "TRANSLATED_SQL", "SOURCE_DIALECT", "TARGET_DIALECT");
        List<List<Object>> rows = new ArrayList<>();
        rows.add(Arrays.asList(
                entry.sqlText,
                response.getExtractedSql(),
                sourceDialect,
                targetDialect
        ));
        return new ArrayResultSet(columns, rows);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private KaiResponse sendToKai(String message, KeboolaConnection connection) throws SQLException {
        KaiClient kai = connection.getKaiClient();
        String chatId = connection.getOrCreateKaiChatId();

        // Determine branch ID (may be null for DuckDB-only connections)
        String branchId = null;
        if (connection.getQueryServiceBackend() != null) {
            branchId = String.valueOf(connection.getQueryServiceBackend().getBranchId());
        }

        KaiResponse response = kai.chat(chatId, message, branchId);
        if (response.hasError()) {
            throw new SQLException("Kai error: " + response.getErrorMessage());
        }
        return response;
    }

    /**
     * Fetches a session log entry by ID from the DuckDB session log table.
     */
    private SessionLogEntry fetchSessionLogEntry(int id, KeboolaConnection connection) throws SQLException {
        DuckDbBackend duckDb = connection.getDuckDbBackend();
        if (duckDb == null) {
            throw new SQLException("Session log is not available (DuckDB backend failed to initialize)");
        }

        connection.getSessionLogger().ensureInitialized();

        String query = "SELECT sql_text, backend, success, error_message FROM "
                + DriverConfig.SESSION_LOG_TABLE + " WHERE id = " + id;
        ExecutionResult result = duckDb.execute(List.of(query));

        if (!result.hasResultSet()) {
            throw new SQLException("Session log query did not return results");
        }

        ResultSet rs = result.getResultSet();
        if (!rs.next()) {
            throw new SQLException("Session log entry #" + id + " not found");
        }

        SessionLogEntry entry = new SessionLogEntry();
        entry.sqlText = rs.getString("sql_text");
        entry.backend = rs.getString("backend");
        entry.success = rs.getBoolean("success");
        entry.errorMessage = rs.getString("error_message");
        rs.close();

        return entry;
    }

    /** Simple holder for session log entry data. */
    static class SessionLogEntry {
        String sqlText;
        String backend;
        boolean success;
        String errorMessage;
    }
}
