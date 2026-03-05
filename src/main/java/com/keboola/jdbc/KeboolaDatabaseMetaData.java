package com.keboola.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.keboola.jdbc.config.DriverConfig;
import com.keboola.jdbc.http.model.TokenInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * DatabaseMetaData implementation for Keboola.
 * Proxies all database object listing to the underlying Snowflake database
 * via SHOW commands executed through the Query Service.
 */
public class KeboolaDatabaseMetaData implements DatabaseMetaData {

    private static final Logger LOG = LoggerFactory.getLogger(KeboolaDatabaseMetaData.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final KeboolaConnection connection;

    public KeboolaDatabaseMetaData(KeboolaConnection connection) {
        this.connection = connection;
    }

    // -- Database identification --

    @Override
    public String getDatabaseProductName() {
        return "Keboola";
    }

    @Override
    public String getDatabaseProductVersion() {
        return DriverConfig.DRIVER_VERSION;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return DriverConfig.MAJOR_VERSION;
    }

    @Override
    public int getDatabaseMinorVersion() {
        return DriverConfig.MINOR_VERSION;
    }

    @Override
    public String getDriverName() {
        return DriverConfig.DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() {
        return DriverConfig.DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return DriverConfig.MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return DriverConfig.MINOR_VERSION;
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 2;
    }

    // -- Catalog/Schema/Table browsing --

    @Override
    public ResultSet getCatalogs() throws SQLException {
        LOG.debug("getCatalogs()");
        List<String> columns = Collections.singletonList("TABLE_CAT");
        List<List<Object>> rows = new ArrayList<>();

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW DATABASES")) {
            int nameIdx = findColumnIndex(rs, "name");
            while (rs.next()) {
                rows.add(Collections.singletonList(rs.getString(nameIdx)));
            }
        }

        rows.sort(Comparator.comparing(r -> (String) r.get(0)));
        return new ArrayResultSet(columns, rows);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        LOG.debug("getSchemas(catalog={}, schemaPattern={})", catalog, schemaPattern);
        List<String> columns = Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG");
        List<List<Object>> rows = new ArrayList<>();

        StringBuilder sql = new StringBuilder("SHOW SCHEMAS");
        if (catalog != null && !catalog.equals("%")) {
            sql.append(" IN DATABASE \"").append(catalog.replace("\"", "\"\"")).append("\"");
        }
        if (schemaPattern != null && !schemaPattern.equals("%")) {
            sql.append(" LIKE '").append(escapeLike(schemaPattern)).append("'");
        }

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql.toString())) {
            int nameIdx = findColumnIndex(rs, "name");
            int dbIdx = findColumnIndex(rs, "database_name");
            while (rs.next()) {
                rows.add(Arrays.asList(rs.getString(nameIdx), rs.getString(dbIdx)));
            }
        }

        rows.sort(Comparator.comparing(r -> (String) r.get(0)));
        return new ArrayResultSet(columns, rows);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        LOG.debug("getTables(catalog={}, schema={}, table={}, types={})", catalog, schemaPattern, tableNamePattern, types != null ? Arrays.toString(types) : "null");

        Set<String> typeSet = null;
        if (types != null) {
            typeSet = new HashSet<>();
            for (String t : types) {
                typeSet.add(t.toUpperCase());
            }
        }

        boolean wantTables = typeSet == null || typeSet.contains("TABLE") || typeSet.contains("BASE TABLE");
        boolean wantViews = typeSet == null || typeSet.contains("VIEW");

        if (!wantTables && !wantViews) {
            return emptyTableResultSet();
        }

        List<String> columns = Arrays.asList(
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
                "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"
        );
        List<List<Object>> rows = new ArrayList<>();

        if (wantTables) {
            collectTables(rows, "SHOW TABLES", catalog, schemaPattern, tableNamePattern, "TABLE");
        }
        if (wantViews) {
            collectTables(rows, "SHOW VIEWS", catalog, schemaPattern, tableNamePattern, "VIEW");
        }

        rows.sort((a, b) -> {
            int cmp = String.valueOf(a.get(3)).compareTo(String.valueOf(b.get(3))); // TABLE_TYPE
            if (cmp != 0) return cmp;
            cmp = String.valueOf(a.get(0)).compareTo(String.valueOf(b.get(0))); // TABLE_CAT
            if (cmp != 0) return cmp;
            cmp = String.valueOf(a.get(1)).compareTo(String.valueOf(b.get(1))); // TABLE_SCHEM
            if (cmp != 0) return cmp;
            return String.valueOf(a.get(2)).compareTo(String.valueOf(b.get(2))); // TABLE_NAME
        });

        return new ArrayResultSet(columns, rows);
    }

    private void collectTables(List<List<Object>> rows, String showCommand,
                               String catalog, String schemaPattern, String tableNamePattern,
                               String tableType) throws SQLException {
        StringBuilder sql = new StringBuilder(showCommand);

        // Scope: IN SCHEMA > IN DATABASE > (unscoped)
        if (schemaPattern != null && !schemaPattern.equals("%") && catalog != null && !catalog.equals("%")) {
            sql.append(" IN SCHEMA \"").append(catalog.replace("\"", "\"\""))
               .append("\".\"").append(schemaPattern.replace("\"", "\"\"")).append("\"");
        } else if (catalog != null && !catalog.equals("%")) {
            sql.append(" IN DATABASE \"").append(catalog.replace("\"", "\"\"")).append("\"");
        } else if (schemaPattern != null && !schemaPattern.equals("%")) {
            sql.append(" IN SCHEMA \"").append(schemaPattern.replace("\"", "\"\"")).append("\"");
        }

        if (tableNamePattern != null && !tableNamePattern.equals("%")) {
            sql.append(" LIKE '").append(escapeLike(tableNamePattern)).append("'");
        }

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql.toString())) {
            int nameIdx = findColumnIndex(rs, "name");
            int schemaIdx = findColumnIndex(rs, "schema_name");
            int dbIdx = findColumnIndex(rs, "database_name");
            while (rs.next()) {
                String schemaName = rs.getString(schemaIdx);
                // Client-side filter for schema pattern when LIKE was used for table name
                if (schemaPattern != null && !schemaPattern.equals("%")
                        && catalog != null && !catalog.equals("%")) {
                    // Already scoped by IN SCHEMA
                } else if (schemaPattern != null && !schemaPattern.equals("%")) {
                    if (!matchesPattern(schemaName, schemaPattern)) continue;
                }

                rows.add(Arrays.asList(
                        rs.getString(dbIdx),    // TABLE_CAT
                        schemaName,             // TABLE_SCHEM
                        rs.getString(nameIdx),  // TABLE_NAME
                        tableType,              // TABLE_TYPE
                        "",                     // REMARKS
                        null, null, null, null, null
                ));
            }
        }
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        LOG.debug("getColumns(catalog={}, schema={}, table={}, column={})", catalog, schemaPattern, tableNamePattern, columnNamePattern);

        List<String> columns = Arrays.asList(
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
                "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
                "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA",
                "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"
        );
        List<List<Object>> rows = new ArrayList<>();

        StringBuilder sql = new StringBuilder("SHOW COLUMNS");

        // Scope: IN TABLE > IN SCHEMA > IN DATABASE > (unscoped)
        if (tableNamePattern != null && !tableNamePattern.equals("%")
                && !tableNamePattern.contains("%") && !tableNamePattern.contains("_")) {
            // Exact table name — scope to table for efficiency
            if (schemaPattern != null && !schemaPattern.equals("%")
                    && !schemaPattern.contains("%") && !schemaPattern.contains("_")) {
                if (catalog != null && !catalog.equals("%")) {
                    sql.append(" IN TABLE \"").append(catalog.replace("\"", "\"\""))
                       .append("\".\"").append(schemaPattern.replace("\"", "\"\""))
                       .append("\".\"").append(tableNamePattern.replace("\"", "\"\"")).append("\"");
                } else {
                    sql.append(" IN TABLE \"").append(schemaPattern.replace("\"", "\"\""))
                       .append("\".\"").append(tableNamePattern.replace("\"", "\"\"")).append("\"");
                }
            } else if (catalog != null && !catalog.equals("%")) {
                sql.append(" IN DATABASE \"").append(catalog.replace("\"", "\"\"")).append("\"");
            }
        } else if (schemaPattern != null && !schemaPattern.equals("%")
                && !schemaPattern.contains("%") && !schemaPattern.contains("_")) {
            if (catalog != null && !catalog.equals("%")) {
                sql.append(" IN SCHEMA \"").append(catalog.replace("\"", "\"\""))
                   .append("\".\"").append(schemaPattern.replace("\"", "\"\"")).append("\"");
            } else {
                sql.append(" IN SCHEMA \"").append(schemaPattern.replace("\"", "\"\"")).append("\"");
            }
        } else if (catalog != null && !catalog.equals("%")) {
            sql.append(" IN DATABASE \"").append(catalog.replace("\"", "\"\"")).append("\"");
        }

        if (columnNamePattern != null && !columnNamePattern.equals("%")) {
            sql.append(" LIKE '").append(escapeLike(columnNamePattern)).append("'");
        }

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql.toString())) {
            int colNameIdx = findColumnIndex(rs, "column_name");
            int dataTypeIdx = findColumnIndex(rs, "data_type");
            int tableNameIdx = findColumnIndex(rs, "table_name");
            int schemaNameIdx = findColumnIndex(rs, "schema_name");
            int dbNameIdx = findColumnIndex(rs, "database_name");
            // Snowflake doesn't return a usable ordinal from SHOW COLUMNS; track per-table
            Map<String, Integer> ordinalTracker = new LinkedHashMap<>();

            while (rs.next()) {
                String tblName = rs.getString(tableNameIdx);
                String schName = rs.getString(schemaNameIdx);
                String dbName = rs.getString(dbNameIdx);

                // Client-side filter for table/schema patterns not handled by scoping
                if (tableNamePattern != null && !tableNamePattern.equals("%")) {
                    if (!matchesPattern(tblName, tableNamePattern)) continue;
                }
                if (schemaPattern != null && !schemaPattern.equals("%")) {
                    if (!matchesPattern(schName, schemaPattern)) continue;
                }

                String dataTypeJson = rs.getString(dataTypeIdx);
                ColumnTypeInfo typeInfo = parseDataType(dataTypeJson);

                String trackKey = dbName + "." + schName + "." + tblName;
                int ordinal = ordinalTracker.merge(trackKey, 1, Integer::sum);

                rows.add(Arrays.asList(
                        dbName,                                         // TABLE_CAT
                        schName,                                        // TABLE_SCHEM
                        tblName,                                        // TABLE_NAME
                        rs.getString(colNameIdx),                       // COLUMN_NAME
                        typeInfo.jdbcType,                              // DATA_TYPE
                        typeInfo.typeName,                              // TYPE_NAME
                        typeInfo.precision,                             // COLUMN_SIZE
                        null,                                           // BUFFER_LENGTH
                        typeInfo.scale,                                 // DECIMAL_DIGITS
                        10,                                             // NUM_PREC_RADIX
                        typeInfo.nullable ? columnNullable : columnNoNulls, // NULLABLE
                        "",                                             // REMARKS
                        null,                                           // COLUMN_DEF
                        0,                                              // SQL_DATA_TYPE
                        0,                                              // SQL_DATETIME_SUB
                        typeInfo.precision,                             // CHAR_OCTET_LENGTH
                        ordinal,                                        // ORDINAL_POSITION
                        typeInfo.nullable ? "YES" : "NO",               // IS_NULLABLE
                        null, null, null, null,                         // SCOPE_*
                        "NO",                                           // IS_AUTOINCREMENT
                        "NO"                                            // IS_GENERATEDCOLUMN
                ));
            }
        }

        return new ArrayResultSet(columns, rows);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        List<String> columns = Collections.singletonList("TABLE_TYPE");
        List<List<Object>> rows = new ArrayList<>();
        rows.add(Collections.singletonList("TABLE"));
        rows.add(Collections.singletonList("VIEW"));
        return new ArrayResultSet(columns, rows);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        List<String> columns = Arrays.asList(
                "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX",
                "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE",
                "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE",
                "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"
        );
        List<List<Object>> rows = new ArrayList<>();

        addTypeInfoRow(rows, "VARCHAR", Types.VARCHAR, 16777216, "'", "'", "length", true, false);
        addTypeInfoRow(rows, "NUMBER", Types.DECIMAL, 38, null, null, "precision,scale", false, false);
        addTypeInfoRow(rows, "INTEGER", Types.INTEGER, 10, null, null, null, false, false);
        addTypeInfoRow(rows, "BIGINT", Types.BIGINT, 19, null, null, null, false, false);
        addTypeInfoRow(rows, "FLOAT", Types.FLOAT, 24, null, null, null, false, false);
        addTypeInfoRow(rows, "DOUBLE", Types.DOUBLE, 53, null, null, null, false, false);
        addTypeInfoRow(rows, "BOOLEAN", Types.BOOLEAN, 1, null, null, null, false, false);
        addTypeInfoRow(rows, "DATE", Types.DATE, 10, "'", "'", null, false, false);
        addTypeInfoRow(rows, "TIME", Types.TIME, 8, "'", "'", null, false, false);
        addTypeInfoRow(rows, "TIMESTAMP", Types.TIMESTAMP, 26, "'", "'", null, false, false);
        addTypeInfoRow(rows, "TIMESTAMP_TZ", Types.TIMESTAMP_WITH_TIMEZONE, 32, "'", "'", null, false, false);
        addTypeInfoRow(rows, "VARIANT", Types.VARCHAR, 16777216, null, null, null, true, false);

        return new ArrayResultSet(columns, rows);
    }

    private void addTypeInfoRow(List<List<Object>> rows, String typeName, int dataType, int precision,
                                 String prefix, String suffix, String createParams, boolean caseSensitive, boolean unsigned) {
        rows.add(Arrays.asList(
                typeName, dataType, precision, prefix, suffix,
                createParams, (int) typeNullable, caseSensitive, (int) typeSearchable, unsigned,
                false, false, typeName, 0, 0, 0, 0, 10
        ));
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"),
                Collections.emptyList()
        );
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyKeysResultSet();
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyKeysResultSet();
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return emptyKeysResultSet();
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER",
                        "INDEX_NAME", "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                        "CARDINALITY", "PAGES", "FILTER_CONDITION"),
                Collections.emptyList()
        );
    }

    // -- SQL support info --

    @Override
    public String getIdentifierQuoteString() {
        return "\"";
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public String getCatalogTerm() {
        return "database";
    }

    @Override
    public String getSchemaTerm() {
        return "schema";
    }

    @Override
    public String getProcedureTerm() {
        return "procedure";
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    @Override
    public String getSQLKeywords() {
        return "";
    }

    @Override
    public String getNumericFunctions() {
        return "ABS,CEIL,FLOOR,ROUND,TRUNC,MOD,SIGN,SQRT,POWER,LOG,LN,EXP";
    }

    @Override
    public String getStringFunctions() {
        return "CONCAT,LENGTH,LOWER,UPPER,TRIM,LTRIM,RTRIM,SUBSTR,REPLACE,LPAD,RPAD";
    }

    @Override
    public String getSystemFunctions() {
        return "CURRENT_DATABASE,CURRENT_SCHEMA,CURRENT_USER,CURRENT_ROLE";
    }

    @Override
    public String getTimeDateFunctions() {
        return "CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,DATEADD,DATEDIFF,EXTRACT";
    }

    // -- Capabilities --

    @Override
    public boolean supportsAlterTableWithAddColumn() { return false; }

    @Override
    public boolean supportsAlterTableWithDropColumn() { return false; }

    @Override
    public boolean supportsColumnAliasing() { return true; }

    @Override
    public boolean supportsGroupBy() { return true; }

    @Override
    public boolean supportsGroupByUnrelated() { return true; }

    @Override
    public boolean supportsGroupByBeyondSelect() { return true; }

    @Override
    public boolean supportsOrderByUnrelated() { return true; }

    @Override
    public boolean supportsSelectForUpdate() { return false; }

    @Override
    public boolean supportsSubqueriesInComparisons() { return true; }

    @Override
    public boolean supportsSubqueriesInExists() { return true; }

    @Override
    public boolean supportsSubqueriesInIns() { return true; }

    @Override
    public boolean supportsSubqueriesInQuantifieds() { return true; }

    @Override
    public boolean supportsCorrelatedSubqueries() { return true; }

    @Override
    public boolean supportsUnion() { return true; }

    @Override
    public boolean supportsUnionAll() { return true; }

    @Override
    public boolean supportsOuterJoins() { return true; }

    @Override
    public boolean supportsFullOuterJoins() { return true; }

    @Override
    public boolean supportsLimitedOuterJoins() { return true; }

    @Override
    public boolean supportsExpressionsInOrderBy() { return true; }

    @Override
    public boolean supportsLikeEscapeClause() { return true; }

    @Override
    public boolean supportsMultipleResultSets() { return false; }

    @Override
    public boolean supportsMultipleTransactions() { return false; }

    @Override
    public boolean supportsNonNullableColumns() { return true; }

    @Override
    public boolean supportsMinimumSQLGrammar() { return true; }

    @Override
    public boolean supportsCoreSQLGrammar() { return true; }

    @Override
    public boolean supportsExtendedSQLGrammar() { return false; }

    @Override
    public boolean supportsANSI92EntryLevelSQL() { return true; }

    @Override
    public boolean supportsANSI92IntermediateSQL() { return false; }

    @Override
    public boolean supportsANSI92FullSQL() { return false; }

    @Override
    public boolean supportsIntegrityEnhancementFacility() { return false; }

    @Override
    public boolean supportsMixedCaseIdentifiers() { return false; }

    @Override
    public boolean storesUpperCaseIdentifiers() { return true; }

    @Override
    public boolean storesLowerCaseIdentifiers() { return false; }

    @Override
    public boolean storesMixedCaseIdentifiers() { return false; }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() { return true; }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() { return false; }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() { return false; }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() { return true; }

    @Override
    public boolean supportsTableCorrelationNames() { return true; }

    @Override
    public boolean supportsDifferentTableCorrelationNames() { return false; }

    @Override
    public boolean supportsTransactions() { return false; }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) { return false; }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() { return false; }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() { return false; }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() { return false; }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() { return false; }

    @Override
    public boolean supportsSchemasInDataManipulation() { return true; }

    @Override
    public boolean supportsSchemasInProcedureCalls() { return false; }

    @Override
    public boolean supportsSchemasInTableDefinitions() { return true; }

    @Override
    public boolean supportsSchemasInIndexDefinitions() { return false; }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() { return false; }

    @Override
    public boolean supportsCatalogsInDataManipulation() { return true; }

    @Override
    public boolean supportsCatalogsInProcedureCalls() { return false; }

    @Override
    public boolean supportsCatalogsInTableDefinitions() { return true; }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() { return false; }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() { return false; }

    @Override
    public boolean supportsPositionedDelete() { return false; }

    @Override
    public boolean supportsPositionedUpdate() { return false; }

    @Override
    public boolean supportsStoredProcedures() { return false; }

    @Override
    public boolean supportsBatchUpdates() { return true; }

    @Override
    public boolean supportsSavepoints() { return false; }

    @Override
    public boolean supportsNamedParameters() { return false; }

    @Override
    public boolean supportsMultipleOpenResults() { return false; }

    @Override
    public boolean supportsGetGeneratedKeys() { return false; }

    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) { return false; }

    @Override
    public int getResultSetHoldability() { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }

    @Override
    public boolean supportsStatementPooling() { return false; }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() { return false; }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() { return false; }

    @Override
    public boolean generatedKeyAlwaysReturned() { return false; }

    // -- Limits --

    @Override
    public int getMaxBinaryLiteralLength() { return 0; }

    @Override
    public int getMaxCharLiteralLength() { return 0; }

    @Override
    public int getMaxColumnNameLength() { return 255; }

    @Override
    public int getMaxColumnsInGroupBy() { return 0; }

    @Override
    public int getMaxColumnsInIndex() { return 0; }

    @Override
    public int getMaxColumnsInOrderBy() { return 0; }

    @Override
    public int getMaxColumnsInSelect() { return 0; }

    @Override
    public int getMaxColumnsInTable() { return 0; }

    @Override
    public int getMaxConnections() { return 0; }

    @Override
    public int getMaxCursorNameLength() { return 0; }

    @Override
    public int getMaxIndexLength() { return 0; }

    @Override
    public int getMaxSchemaNameLength() { return 255; }

    @Override
    public int getMaxProcedureNameLength() { return 0; }

    @Override
    public int getMaxCatalogNameLength() { return 255; }

    @Override
    public int getMaxRowSize() { return 0; }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() { return false; }

    @Override
    public int getMaxStatementLength() { return 0; }

    @Override
    public int getMaxStatements() { return 0; }

    @Override
    public int getMaxTableNameLength() { return 255; }

    @Override
    public int getMaxTablesInSelect() { return 0; }

    @Override
    public int getMaxUserNameLength() { return 0; }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    // -- Other required methods --

    @Override
    public boolean isReadOnly() { return true; }

    @Override
    public boolean nullsAreSortedHigh() { return true; }

    @Override
    public boolean nullsAreSortedLow() { return false; }

    @Override
    public boolean nullsAreSortedAtStart() { return false; }

    @Override
    public boolean nullsAreSortedAtEnd() { return false; }

    @Override
    public boolean usesLocalFiles() { return false; }

    @Override
    public boolean usesLocalFilePerTable() { return false; }

    @Override
    public boolean nullPlusNonNullIsNull() { return true; }

    @Override
    public boolean supportsConvert() { return false; }

    @Override
    public boolean supportsConvert(int fromType, int toType) { return false; }

    @Override
    public boolean allProceduresAreCallable() { return false; }

    @Override
    public boolean allTablesAreSelectable() { return true; }

    @Override
    public String getURL() {
        return "jdbc:keboola://" + connection.getHost();
    }

    @Override
    public String getUserName() {
        TokenInfo tokenInfo = connection.getTokenInfo();
        return tokenInfo != null ? tokenInfo.getDescription() : null;
    }

    @Override
    public boolean isCatalogAtStart() { return true; }

    @Override
    public boolean ownUpdatesAreVisible(int type) { return false; }

    @Override
    public boolean ownDeletesAreVisible(int type) { return false; }

    @Override
    public boolean ownInsertsAreVisible(int type) { return false; }

    @Override
    public boolean othersUpdatesAreVisible(int type) { return false; }

    @Override
    public boolean othersDeletesAreVisible(int type) { return false; }

    @Override
    public boolean othersInsertsAreVisible(int type) { return false; }

    @Override
    public boolean updatesAreDetected(int type) { return false; }

    @Override
    public boolean deletesAreDetected(int type) { return false; }

    @Override
    public boolean insertsAreDetected(int type) { return false; }

    @Override
    public boolean locatorsUpdateCopy() { return false; }

    @Override
    public RowIdLifetime getRowIdLifetime() { return RowIdLifetime.ROWID_UNSUPPORTED; }

    @Override
    public int getSQLStateType() { return sqlStateSQL; }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "RESERVED1",
                        "RESERVED2", "RESERVED3", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"),
                Collections.emptyList()
        );
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME",
                        "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE",
                        "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE",
                        "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
                Collections.emptyList()
        );
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                        "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"),
                Collections.emptyList()
        );
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                        "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"),
                Collections.emptyList()
        );
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                        "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"),
                Collections.emptyList()
        );
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                        "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"),
                Collections.emptyList()
        );
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME",
                        "DATA_TYPE", "REMARKS", "BASE_TYPE"),
                Collections.emptyList()
        );
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
                        "SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME"),
                Collections.emptyList()
        );
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME"),
                Collections.emptyList()
        );
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME",
                        "DATA_TYPE", "ATTR_TYPE_NAME", "ATTR_SIZE", "DECIMAL_DIGITS",
                        "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "ATTR_DEF",
                        "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                        "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
                        "SOURCE_DATA_TYPE"),
                Collections.emptyList()
        );
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION"),
                Collections.emptyList()
        );
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS",
                        "FUNCTION_TYPE", "SPECIFIC_NAME"),
                Collections.emptyList()
        );
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME",
                        "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH",
                        "SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH",
                        "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
                Collections.emptyList()
        );
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                        "DATA_TYPE", "COLUMN_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
                        "COLUMN_USAGE", "REMARKS", "CHAR_OCTET_LENGTH", "IS_NULLABLE"),
                Collections.emptyList()
        );
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    // -- Wrapper --

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    // -- Helpers --

    /**
     * SQL LIKE pattern matching: % = any, _ = single char, null = match all.
     */
    private boolean matchesPattern(String value, String pattern) {
        if (pattern == null || pattern.equals("%")) {
            return true;
        }
        String regex = pattern
                .replace("\\", "\\\\")
                .replace(".", "\\.")
                .replace("(", "\\(")
                .replace(")", "\\)")
                .replace("[", "\\[")
                .replace("]", "\\]")
                .replace("{", "\\{")
                .replace("}", "\\}")
                .replace("^", "\\^")
                .replace("$", "\\$")
                .replace("|", "\\|")
                .replace("+", "\\+")
                .replace("*", "\\*")
                .replace("?", "\\?")
                .replace("%", ".*")
                .replace("_", ".");
        return value.matches("(?i)" + regex);
    }

    /**
     * Escapes a SQL LIKE pattern value for embedding in a SHOW ... LIKE 'pattern' clause.
     * Converts JDBC % and _ wildcards to Snowflake LIKE syntax (which also uses % and _).
     */
    private String escapeLike(String pattern) {
        return pattern.replace("'", "''");
    }

    /**
     * Finds the 1-based column index for a column name in the result set (case-insensitive).
     */
    private int findColumnIndex(ResultSet rs, String columnName) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (meta.getColumnName(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        throw new SQLException("Column '" + columnName + "' not found in SHOW result");
    }

    /**
     * Parses the JSON data_type field from SHOW COLUMNS output.
     * Example: {"type":"NUMBER","precision":38,"scale":0,"nullable":true}
     */
    private ColumnTypeInfo parseDataType(String dataTypeJson) {
        ColumnTypeInfo info = new ColumnTypeInfo();
        if (dataTypeJson == null || dataTypeJson.isEmpty()) {
            info.typeName = "VARCHAR";
            info.jdbcType = Types.VARCHAR;
            info.precision = 16777216;
            info.scale = 0;
            info.nullable = true;
            return info;
        }

        try {
            JsonNode node = OBJECT_MAPPER.readTree(dataTypeJson);
            info.typeName = node.has("type") ? node.get("type").asText("VARCHAR") : "VARCHAR";
            info.nullable = !node.has("nullable") || node.get("nullable").asBoolean(true);

            switch (info.typeName.toUpperCase()) {
                case "NUMBER":
                case "DECIMAL":
                case "NUMERIC":
                    info.jdbcType = Types.DECIMAL;
                    info.precision = node.has("precision") ? node.get("precision").asInt(38) : 38;
                    info.scale = node.has("scale") ? node.get("scale").asInt(0) : 0;
                    break;
                case "INT":
                case "INTEGER":
                    info.jdbcType = Types.INTEGER;
                    info.precision = 10;
                    info.scale = 0;
                    break;
                case "BIGINT":
                    info.jdbcType = Types.BIGINT;
                    info.precision = 19;
                    info.scale = 0;
                    break;
                case "FLOAT":
                case "FLOAT4":
                    info.jdbcType = Types.FLOAT;
                    info.precision = node.has("precision") ? node.get("precision").asInt(24) : 24;
                    info.scale = 0;
                    break;
                case "DOUBLE":
                case "DOUBLE PRECISION":
                case "FLOAT8":
                    info.jdbcType = Types.DOUBLE;
                    info.precision = node.has("precision") ? node.get("precision").asInt(53) : 53;
                    info.scale = 0;
                    break;
                case "BOOLEAN":
                    info.jdbcType = Types.BOOLEAN;
                    info.precision = 1;
                    info.scale = 0;
                    break;
                case "DATE":
                    info.jdbcType = Types.DATE;
                    info.precision = 10;
                    info.scale = 0;
                    break;
                case "TIME":
                    info.jdbcType = Types.TIME;
                    info.precision = node.has("precision") ? node.get("precision").asInt(9) : 9;
                    info.scale = 0;
                    break;
                case "TIMESTAMP_NTZ":
                case "TIMESTAMP":
                    info.jdbcType = Types.TIMESTAMP;
                    info.precision = node.has("precision") ? node.get("precision").asInt(9) : 9;
                    info.scale = 0;
                    break;
                case "TIMESTAMP_TZ":
                case "TIMESTAMP_LTZ":
                    info.jdbcType = Types.TIMESTAMP_WITH_TIMEZONE;
                    info.precision = node.has("precision") ? node.get("precision").asInt(9) : 9;
                    info.scale = 0;
                    break;
                case "BINARY":
                case "VARBINARY":
                    info.jdbcType = Types.BINARY;
                    info.precision = node.has("length") ? node.get("length").asInt(8388608) : 8388608;
                    info.scale = 0;
                    break;
                case "VARIANT":
                case "OBJECT":
                case "ARRAY":
                    info.jdbcType = Types.VARCHAR;
                    info.precision = 16777216;
                    info.scale = 0;
                    break;
                default:
                    // VARCHAR, CHAR, TEXT, STRING, etc.
                    info.jdbcType = Types.VARCHAR;
                    info.precision = node.has("length") ? node.get("length").asInt(16777216) : 16777216;
                    if (info.precision <= 0) info.precision = 16777216;
                    info.scale = 0;
                    break;
            }
        } catch (Exception e) {
            LOG.warn("Failed to parse data_type JSON: {}", dataTypeJson, e);
            info.typeName = "VARCHAR";
            info.jdbcType = Types.VARCHAR;
            info.precision = 16777216;
            info.scale = 0;
            info.nullable = true;
        }
        return info;
    }

    private static class ColumnTypeInfo {
        String typeName;
        int jdbcType;
        int precision;
        int scale;
        boolean nullable;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    private ResultSet emptyTableResultSet() throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
                        "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"),
                Collections.emptyList()
        );
    }

    private ResultSet emptyKeysResultSet() throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                        "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
                        "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"),
                Collections.emptyList()
        );
    }
}
