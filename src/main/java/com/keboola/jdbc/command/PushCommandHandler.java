package com.keboola.jdbc.command;

import com.keboola.jdbc.ArrayResultSet;
import com.keboola.jdbc.KeboolaConnection;
import com.keboola.jdbc.backend.DuckDbBackend;
import com.keboola.jdbc.backend.ExecutionResult;
import com.keboola.jdbc.backend.QueryServiceBackend;
import com.keboola.jdbc.config.DriverConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles {@code KEBOOLA PUSH TABLE} commands that transfer data from the local
 * DuckDB backend into Snowflake via the Query Service.
 *
 * <p>Supported syntax:
 * <ul>
 *   <li>{@code KEBOOLA PUSH TABLE local_table} — pushes DuckDB table to Snowflake
 *       using the same table name in the current schema</li>
 *   <li>{@code KEBOOLA PUSH TABLE local_table INTO stage.bucket.table} — pushes
 *       into a specific Keboola table (schema = stage.bucket)</li>
 * </ul>
 *
 * <p>All column types are mapped to VARCHAR on the Snowflake side for simplicity.
 * Data is inserted in batches using multi-row INSERT VALUES syntax.
 */
public class PushCommandHandler implements KeboolaCommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PushCommandHandler.class);

    /**
     * Matches KEBOOLA PUSH TABLE localTable [INTO stage.bucket.table].
     * Group 1: local DuckDB table name
     * Groups 2-4: optional stage.bucket.table target
     */
    private static final Pattern PUSH_PATTERN = Pattern.compile(
            "^\\s*KEBOOLA\\s+PUSH\\s+TABLE\\s+\"?(\\w+)\"?" +
            "(?:\\s+INTO\\s+\"?([\\w.-]+)\"?\\s*\\.\\s*\"?([\\w.-]+)\"?\\s*\\.\\s*\"?([\\w.-]+)\"?)?" +
            "\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );

    @Override
    public boolean canHandle(String sql) {
        if (sql == null) return false;
        return PUSH_PATTERN.matcher(sql.trim()).matches();
    }

    @Override
    public ResultSet execute(String sql, KeboolaConnection connection) throws SQLException {
        String trimmed = sql.trim();

        Matcher matcher = PUSH_PATTERN.matcher(trimmed);
        if (!matcher.matches()) {
            throw new SQLException("Failed to parse PUSH command: " + sql);
        }

        String localTable = matcher.group(1);
        String stage = matcher.group(2);
        String bucket = matcher.group(3);
        String tableName = matcher.group(4);

        // Validate backends
        QueryServiceBackend qsBackend = connection.getQueryServiceBackend();
        if (qsBackend == null) {
            throw new SQLException(
                    "KEBOOLA PUSH requires a Keboola token (Query Service backend). "
                    + "Connect with token=xxx to use PUSH commands.");
        }

        DuckDbBackend duckDb = connection.getDuckDbBackend();
        if (duckDb == null) {
            throw new SQLException(
                    "KEBOOLA PUSH requires a DuckDB backend. "
                    + "Connect with backend=duckdb or duckdbPath=... to use PUSH commands.");
        }

        // Build target table reference
        String targetRef;
        String targetDisplay;
        if (stage != null && bucket != null && tableName != null) {
            // INTO stage.bucket.table -> schema = "stage.bucket", table = "table"
            String schemaName = stage + "." + bucket;
            targetRef = "\"" + schemaName.replace("\"", "\"\"") + "\".\""
                    + tableName.replace("\"", "\"\"") + "\"";
            targetDisplay = schemaName + "." + tableName;
        } else {
            // No INTO clause: use local table name directly
            targetRef = "\"" + localTable.replace("\"", "\"\"") + "\"";
            targetDisplay = localTable;
        }

        LOG.info("PUSH TABLE: DuckDB \"{}\" -> Snowflake {}", localTable, targetDisplay);

        // Step 1: Read data from DuckDB
        String selectSql = "SELECT * FROM \"" + localTable.replace("\"", "\"\"") + "\"";
        ExecutionResult duckResult = duckDb.execute(List.of(selectSql));
        if (!duckResult.hasResultSet()) {
            throw new SQLException("Failed to read from DuckDB table: " + localTable);
        }

        long rowCount;
        try (ResultSet sourceRs = duckResult.getResultSet()) {
            ResultSetMetaData meta = sourceRs.getMetaData();
            int columnCount = meta.getColumnCount();

            // Step 2: Build CREATE TABLE IF NOT EXISTS on Snowflake (all VARCHAR)
            String createSql = buildCreateTableSql(targetRef, meta);
            LOG.info("Creating Snowflake table: {}", createSql);
            qsBackend.execute(List.of(createSql));

            // Step 3: Insert data in batches
            rowCount = 0;
            List<String> batchValues = new ArrayList<>();

            while (sourceRs.next()) {
                StringBuilder rowValues = new StringBuilder("(");
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) rowValues.append(", ");
                    String value = sourceRs.getString(i);
                    if (value == null) {
                        rowValues.append("NULL");
                    } else {
                        // Escape single quotes
                        rowValues.append("'").append(value.replace("'", "''")).append("'");
                    }
                }
                rowValues.append(")");
                batchValues.add(rowValues.toString());
                rowCount++;

                if (batchValues.size() >= DriverConfig.PUSH_BATCH_SIZE) {
                    executeInsertBatch(qsBackend, targetRef, meta, batchValues);
                    batchValues.clear();
                    LOG.debug("Inserted {} rows so far", rowCount);
                }
            }

            // Flush remaining batch
            if (!batchValues.isEmpty()) {
                executeInsertBatch(qsBackend, targetRef, meta, batchValues);
            }
        }

        LOG.info("PUSH completed: {} rows inserted into Snowflake table {}", rowCount, targetDisplay);

        // Return status ResultSet
        List<String> columns = Arrays.asList("STATUS", "TABLE", "ROWS", "MESSAGE");
        List<List<Object>> rows = Collections.singletonList(
                Arrays.asList("OK", targetDisplay, rowCount,
                        "Pushed " + rowCount + " rows into table '" + targetDisplay + "'")
        );
        return new ArrayResultSet(columns, rows);
    }

    /**
     * Builds a CREATE TABLE IF NOT EXISTS statement with all VARCHAR columns.
     */
    private String buildCreateTableSql(String targetRef, ResultSetMetaData meta) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ").append(targetRef).append(" (");

        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (i > 1) sb.append(", ");
            String colName = meta.getColumnName(i);
            sb.append("\"").append(colName.replace("\"", "\"\"")).append("\" VARCHAR");
        }

        sb.append(")");
        return sb.toString();
    }

    /**
     * Executes a batch INSERT statement via Query Service using multi-row VALUES syntax.
     */
    private void executeInsertBatch(QueryServiceBackend qsBackend, String targetRef,
                                     ResultSetMetaData meta, List<String> batchValues)
            throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(targetRef).append(" (");

        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (i > 1) sb.append(", ");
            String colName = meta.getColumnName(i);
            sb.append("\"").append(colName.replace("\"", "\"\"")).append("\"");
        }

        sb.append(") VALUES ");

        for (int i = 0; i < batchValues.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(batchValues.get(i));
        }

        qsBackend.execute(List.of(sb.toString()));
    }
}
