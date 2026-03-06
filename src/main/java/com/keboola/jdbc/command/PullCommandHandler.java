package com.keboola.jdbc.command;

import com.keboola.jdbc.ArrayResultSet;
import com.keboola.jdbc.KeboolaConnection;
import com.keboola.jdbc.backend.DuckDbBackend;
import com.keboola.jdbc.backend.ExecutionResult;
import com.keboola.jdbc.backend.QueryServiceBackend;
import com.keboola.jdbc.config.DriverConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles {@code KEBOOLA PULL} commands that transfer data from Keboola Query Service
 * into the local DuckDB backend.
 *
 * <p>Supported syntax:
 * <ul>
 *   <li>{@code KEBOOLA PULL TABLE bucket.table} — pulls entire table into DuckDB</li>
 *   <li>{@code KEBOOLA PULL TABLE bucket.table INTO local_name} — pulls with custom DuckDB table name</li>
 *   <li>{@code KEBOOLA PULL QUERY SELECT ... INTO local_name} — executes query via Query Service,
 *       materializes results into DuckDB table</li>
 * </ul>
 *
 * <p>Requires both Query Service backend (token) and DuckDB backend to be available.
 */
public class PullCommandHandler implements KeboolaCommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PullCommandHandler.class);

    /**
     * Matches KEBOOLA PULL TABLE tableId [INTO alias].
     * Table ID format: stage.bucket.table (e.g. in.c-main.orders)
     */
    private static final Pattern PULL_TABLE_PATTERN = Pattern.compile(
            "^\\s*KEBOOLA\\s+PULL\\s+TABLE\\s+" +
            "\"?([\\w.-]+)\"?\\s*\\.\\s*\"?([\\w.-]+)\"?\\s*\\.\\s*\"?([\\w.-]+)\"?" +
            "(?:\\s+INTO\\s+\"?([\\w]+)\"?)?" +
            "\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Matches KEBOOLA PULL QUERY ... INTO alias.
     * The query can be any SQL, and the alias is required.
     */
    private static final Pattern PULL_QUERY_PATTERN = Pattern.compile(
            "^\\s*KEBOOLA\\s+PULL\\s+QUERY\\s+(.+?)\\s+INTO\\s+\"?([\\w]+)\"?\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    /** Batch size for INSERT operations into DuckDB. */
    private static final int BATCH_SIZE = 1000;

    @Override
    public boolean canHandle(String sql) {
        if (sql == null) return false;
        String trimmed = sql.trim();
        return PULL_TABLE_PATTERN.matcher(trimmed).matches()
                || PULL_QUERY_PATTERN.matcher(trimmed).matches();
    }

    @Override
    public ResultSet execute(String sql, KeboolaConnection connection) throws SQLException {
        String trimmed = sql.trim();

        Matcher tableMatcher = PULL_TABLE_PATTERN.matcher(trimmed);
        if (tableMatcher.matches()) {
            return executePullTable(tableMatcher, connection);
        }

        Matcher queryMatcher = PULL_QUERY_PATTERN.matcher(trimmed);
        if (queryMatcher.matches()) {
            return executePullQuery(queryMatcher, connection);
        }

        throw new SQLException("Failed to parse PULL command: " + sql);
    }

    /**
     * Handles KEBOOLA PULL TABLE stage.bucket.table [INTO alias].
     */
    private ResultSet executePullTable(Matcher matcher, KeboolaConnection connection)
            throws SQLException {
        String stage = matcher.group(1);     // e.g. "in"
        String bucket = matcher.group(2);    // e.g. "c-main"
        String tableName = matcher.group(3); // e.g. "orders"
        String alias = matcher.group(4);     // nullable

        // Build the Snowflake-style schema.table reference
        String schemaName = stage + "." + bucket;   // e.g. "in.c-main"
        String targetName = alias != null ? alias : tableName;

        LOG.info("PULL TABLE: source=\"{}\".\"{}\" -> DuckDB table \"{}\"",
                schemaName, tableName, targetName);

        // Execute SELECT * via Query Service
        String selectSql = "SELECT * FROM \"" + schemaName.replace("\"", "\"\"")
                + "\".\"" + tableName.replace("\"", "\"\"") + "\"";

        return pullViaQueryService(selectSql, targetName, connection);
    }

    /**
     * Handles KEBOOLA PULL QUERY ... INTO alias.
     */
    private ResultSet executePullQuery(Matcher matcher, KeboolaConnection connection)
            throws SQLException {
        String querySql = matcher.group(1).trim();
        String targetName = matcher.group(2);

        LOG.info("PULL QUERY: sql=\"{}\" -> DuckDB table \"{}\"",
                querySql.length() > 100 ? querySql.substring(0, 100) + "..." : querySql,
                targetName);

        return pullViaQueryService(querySql, targetName, connection);
    }

    /**
     * Core logic: executes SQL via Query Service, materializes the result into a DuckDB table.
     *
     * @param selectSql  the SQL to execute on Query Service
     * @param targetName the DuckDB table name to create
     * @param connection the Keboola connection (provides both backends)
     * @return a ResultSet with the operation status
     */
    private ResultSet pullViaQueryService(String selectSql, String targetName,
                                          KeboolaConnection connection) throws SQLException {
        // Validate both backends are available
        QueryServiceBackend qsBackend = connection.getQueryServiceBackend();
        if (qsBackend == null) {
            throw new SQLException(
                    "KEBOOLA PULL requires a Keboola token (Query Service backend). "
                    + "Connect with token=xxx to use PULL commands.");
        }

        DuckDbBackend duckDb = connection.getDuckDbBackend();
        if (duckDb == null) {
            // Lazy-create DuckDB — switchBackend changes active backend, so restore it after
            connection.switchBackend(DriverConfig.BACKEND_DUCKDB);
            duckDb = connection.getDuckDbBackend();
            connection.switchBackend(DriverConfig.BACKEND_QUERY_SERVICE);
        }
        if (duckDb == null) {
            throw new SQLException(
                    "KEBOOLA PULL requires a DuckDB backend. "
                    + "Connect with backend=duckdb or duckdbPath=... to use PULL commands.");
        }

        // Step 1: Execute query via Query Service
        LOG.info("Executing query on Query Service: {}", selectSql);
        ExecutionResult qsResult = qsBackend.execute(List.of(selectSql));
        if (!qsResult.hasResultSet()) {
            throw new SQLException("PULL source query did not return a result set");
        }

        long rowCount;
        try (ResultSet sourceRs = qsResult.getResultSet()) {
            ResultSetMetaData meta = sourceRs.getMetaData();
            int columnCount = meta.getColumnCount();

            // Step 2: Build CREATE TABLE DDL for DuckDB
            String createSql = buildCreateTableSql(targetName, meta);
            LOG.info("Creating DuckDB table: {}", createSql);
            duckDb.execute(List.of("DROP TABLE IF EXISTS \"" + targetName.replace("\"", "\"\"") + "\""));
            duckDb.execute(List.of(createSql));

            // Step 3: Insert data in batches
            String insertSql = buildInsertSql(targetName, columnCount);
            Connection nativeConn = duckDb.getNativeConnection();
            nativeConn.setAutoCommit(false);

            try (PreparedStatement insertStmt = nativeConn.prepareStatement(insertSql)) {
                rowCount = 0;
                int batchCount = 0;

                while (sourceRs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        String value = sourceRs.getString(i);
                        if (value == null) {
                            insertStmt.setNull(i, Types.VARCHAR);
                        } else {
                            insertStmt.setString(i, value);
                        }
                    }
                    insertStmt.addBatch();
                    batchCount++;
                    rowCount++;

                    if (batchCount >= BATCH_SIZE) {
                        insertStmt.executeBatch();
                        batchCount = 0;
                        LOG.debug("Inserted {} rows so far", rowCount);
                    }
                }

                // Flush remaining batch
                if (batchCount > 0) {
                    insertStmt.executeBatch();
                }

                nativeConn.commit();
            } catch (SQLException e) {
                nativeConn.rollback();
                throw e;
            } finally {
                nativeConn.setAutoCommit(true);
            }
        }

        LOG.info("PULL completed: {} rows inserted into DuckDB table \"{}\"", rowCount, targetName);

        // Return status ResultSet
        List<String> columns = Arrays.asList("STATUS", "TABLE", "ROWS", "MESSAGE");
        List<List<Object>> rows = Collections.singletonList(
                Arrays.asList("OK", targetName, rowCount,
                        "Pulled " + rowCount + " rows into table '" + targetName + "'")
        );
        return new ArrayResultSet(columns, rows);
    }

    /**
     * Builds a CREATE TABLE statement for DuckDB based on the source ResultSet metadata.
     * Maps Snowflake/JDBC types to DuckDB-compatible types.
     */
    private String buildCreateTableSql(String tableName, ResultSetMetaData meta) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE \"").append(tableName.replace("\"", "\"\"")).append("\" (");

        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (i > 1) sb.append(", ");
            String colName = meta.getColumnName(i);
            String duckType = mapToDuckDbType(meta.getColumnType(i), meta.getColumnTypeName(i),
                    meta.getPrecision(i), meta.getScale(i));
            sb.append("\"").append(colName.replace("\"", "\"\"")).append("\" ").append(duckType);
        }

        sb.append(")");
        return sb.toString();
    }

    /**
     * Builds a parameterized INSERT statement for the given table and column count.
     */
    private String buildInsertSql(String tableName, int columnCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO \"").append(tableName.replace("\"", "\"\"")).append("\" VALUES (");
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) sb.append(", ");
            sb.append("?");
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Maps JDBC type codes to DuckDB-compatible type names.
     * Since we transfer data as strings and DuckDB has good implicit casting,
     * we use VARCHAR for most types and rely on DuckDB's type system.
     */
    private String mapToDuckDbType(int jdbcType, String typeName, int precision, int scale) {
        if (typeName != null) {
            String upper = typeName.toUpperCase();

            // Snowflake-specific types
            switch (upper) {
                case "NUMBER":
                case "DECIMAL":
                case "NUMERIC":
                    if (scale > 0) {
                        return "DECIMAL(" + precision + ", " + scale + ")";
                    }
                    if (precision > 0 && precision <= 9) return "INTEGER";
                    if (precision > 0 && precision <= 18) return "BIGINT";
                    return "HUGEINT";

                case "FLOAT":
                case "FLOAT4":
                case "REAL":
                    return "FLOAT";

                case "DOUBLE":
                case "FLOAT8":
                case "DOUBLE PRECISION":
                    return "DOUBLE";

                case "INTEGER":
                case "INT":
                    return "INTEGER";

                case "BIGINT":
                    return "BIGINT";

                case "SMALLINT":
                    return "SMALLINT";

                case "TINYINT":
                    return "TINYINT";

                case "BOOLEAN":
                    return "BOOLEAN";

                case "DATE":
                case "TIME":
                case "TIMESTAMP":
                case "TIMESTAMP_NTZ":
                case "DATETIME":
                case "TIMESTAMP_TZ":
                case "TIMESTAMP_LTZ":
                case "TIMESTAMPTZ":
                    // Snowflake Query Service returns timestamps as strings in various formats
                    // (epoch seconds, ISO 8601, etc.) — use VARCHAR to avoid DuckDB parse errors
                    return "VARCHAR";

                case "VARIANT":
                case "OBJECT":
                case "ARRAY":
                    return "VARCHAR";

                case "BINARY":
                case "VARBINARY":
                    return "BLOB";
            }
        }

        // Fallback: map by JDBC type code
        switch (jdbcType) {
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return "INTEGER";
            case Types.BIGINT:
                return "BIGINT";
            case Types.FLOAT:
            case Types.REAL:
                return "FLOAT";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.DECIMAL:
            case Types.NUMERIC:
                if (scale > 0) return "DECIMAL(" + precision + ", " + scale + ")";
                return "BIGINT";
            case Types.BOOLEAN:
            case Types.BIT:
                return "BOOLEAN";
            case Types.DATE:
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                // Snowflake Query Service may return dates/timestamps as epoch strings
                // — use VARCHAR to avoid DuckDB parse errors
                return "VARCHAR";
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return "BLOB";
            default:
                return "VARCHAR";
        }
    }
}
