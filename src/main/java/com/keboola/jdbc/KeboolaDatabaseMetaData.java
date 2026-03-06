package com.keboola.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.keboola.jdbc.command.VirtualTableMetadata;
import com.keboola.jdbc.config.DriverConfig;
import com.keboola.jdbc.meta.TypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * DatabaseMetaData implementation for Keboola.
 * Proxies database object listing to the underlying Snowflake database using SHOW commands
 * with LIKE clauses, IN scoping, and client-side regex pattern filtering.
 * Also exposes virtual _keboola.* tables for Keboola platform metadata.
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
                String dbName = rs.getString(nameIdx);
                if (isFilteredDatabase(dbName)) continue;
                rows.add(Collections.singletonList(dbName));
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

        if ("".equals(catalog)) {
            return new ArrayResultSet(columns, Collections.emptyList());
        }

        List<List<Object>> rows = new ArrayList<>();

        // Add virtual _keboola schema
        String currentCatalog = connection.getCatalog();
        if (matchesPattern(VirtualTableMetadata.SCHEMA_NAME, schemaPattern)) {
            String vtCatalog = currentCatalog != null ? currentCatalog : "Keboola";
            if (catalog == null || matchesPattern(vtCatalog, catalog)) {
                rows.add(Arrays.asList(VirtualTableMetadata.SCHEMA_NAME, vtCatalog));
            }
        }

        // Real schemas from Snowflake via SHOW SCHEMAS
        StringBuilder sql = new StringBuilder("SHOW SCHEMAS");
        if (schemaPattern != null && !schemaPattern.isEmpty() && !schemaPattern.equals("%")) {
            sql.append(" LIKE '").append(escapeSingleQuoteForLike(schemaPattern)).append("'");
        }
        sql.append(buildCatalogInClause(catalog));

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql.toString())) {
            int nameIdx = findColumnIndex(rs, "name");
            int dbIdx = findColumnIndex(rs, "database_name");
            while (rs.next()) {
                String schemaName = rs.getString(nameIdx);
                String dbName = rs.getString(dbIdx);
                if (isFilteredDatabase(dbName)) continue;
                if (matchesPattern(dbName, catalog)) {
                    rows.add(Arrays.asList(schemaName, dbName));
                }
            }
        }

        rows.sort(Comparator.<List<Object>, String>comparing(r -> (String) r.get(1))
                .thenComparing(r -> (String) r.get(0)));
        return new ArrayResultSet(columns, rows);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        LOG.debug("getTables(catalog={}, schema={}, table={}, types={})", catalog, schemaPattern, tableNamePattern, types != null ? Arrays.toString(types) : "null");

        List<String> columns = Arrays.asList(
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
                "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"
        );

        if ("".equals(catalog) || "".equals(schemaPattern)) {
            return new ArrayResultSet(columns, Collections.emptyList());
        }

        Set<String> typeSet = null;
        if (types != null) {
            typeSet = new HashSet<>();
            for (String t : types) {
                typeSet.add(t.toUpperCase());
            }
        }

        boolean wantTables = typeSet == null || typeSet.contains("TABLE") || typeSet.contains("BASE TABLE");
        boolean wantViews = typeSet == null || typeSet.contains("VIEW");
        boolean wantVirtual = typeSet == null || typeSet.contains("VIRTUAL TABLE") || typeSet.contains("TABLE");

        List<List<Object>> rows = new ArrayList<>();

        // Add virtual _keboola tables
        if (wantVirtual && matchesPattern(VirtualTableMetadata.SCHEMA_NAME, schemaPattern)) {
            String currentCatalog = connection.getCatalog();
            String vtCatalog = currentCatalog != null ? currentCatalog : "Keboola";
            if (catalog == null || matchesPattern(vtCatalog, catalog)) {
                for (String vtName : VirtualTableMetadata.getTableNames()) {
                    if (matchesPattern(vtName, tableNamePattern)) {
                        rows.add(Arrays.asList(
                                vtCatalog,
                                VirtualTableMetadata.SCHEMA_NAME,
                                vtName,
                                "VIRTUAL TABLE",
                                "Keboola platform metadata",
                                null, null, null, null, null
                        ));
                    }
                }
            }
        }

        // Real tables/views from Snowflake (skip if schema is exclusively _keboola)
        boolean schemaIsOnlyVirtual = VirtualTableMetadata.SCHEMA_NAME.equalsIgnoreCase(schemaPattern);
        if ((wantTables || wantViews) && !schemaIsOnlyVirtual) {
            String showCommand;
            if (wantTables && wantViews) {
                showCommand = "SHOW OBJECTS";
            } else if (wantViews) {
                showCommand = "SHOW VIEWS";
            } else {
                showCommand = "SHOW TABLES";
            }

            StringBuilder sql = new StringBuilder(showCommand);
            if (tableNamePattern != null && !tableNamePattern.isEmpty() && !tableNamePattern.equals("%")) {
                sql.append(" LIKE '").append(escapeSingleQuoteForLike(tableNamePattern)).append("'");
            }
            sql.append(buildScopeInClause(catalog, schemaPattern));

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql.toString())) {
                int nameIdx = findColumnIndex(rs, "name");
                int schemaIdx = findColumnIndex(rs, "schema_name");
                int dbIdx = findColumnIndex(rs, "database_name");
                int kindIdx = findColumnIndex(rs, "kind");
                while (rs.next()) {
                    String schemaName = rs.getString(schemaIdx);
                    String dbName = rs.getString(dbIdx);
                    String kind = rs.getString(kindIdx);

                    if (isFilteredDatabase(dbName)) continue;
                    if (!matchesPattern(schemaName, schemaPattern)) continue;
                    if (!matchesPattern(dbName, catalog)) continue;

                    String tableType = "TABLE";
                    if ("VIEW".equalsIgnoreCase(kind)) {
                        tableType = "VIEW";
                    }

                    if (typeSet != null && !typeSet.contains(tableType) && !typeSet.contains("BASE TABLE")) {
                        continue;
                    }

                    rows.add(Arrays.asList(
                            dbName,
                            schemaName,
                            rs.getString(nameIdx),
                            tableType,
                            "",
                            null, null, null, null, null
                    ));
                }
            }
        }

        rows.sort(Comparator.<List<Object>, String>comparing(r -> (String) r.get(3))  // TABLE_TYPE
                .thenComparing(r -> (String) r.get(0))   // TABLE_CAT
                .thenComparing(r -> (String) r.get(1))   // TABLE_SCHEM
                .thenComparing(r -> (String) r.get(2)));  // TABLE_NAME
        return new ArrayResultSet(columns, rows);
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

        if ("".equals(catalog) || "".equals(schemaPattern)) {
            return new ArrayResultSet(columns, Collections.emptyList());
        }

        List<List<Object>> rows = new ArrayList<>();

        // Add columns for virtual _keboola tables
        if (matchesPattern(VirtualTableMetadata.SCHEMA_NAME, schemaPattern)) {
            String currentCatalog = connection.getCatalog();
            String vtCatalog = currentCatalog != null ? currentCatalog : "Keboola";
            if (catalog == null || matchesPattern(vtCatalog, catalog)) {
                for (String vtName : VirtualTableMetadata.getTableNames()) {
                    if (!matchesPattern(vtName, tableNamePattern)) continue;
                    List<Object[]> vtCols = VirtualTableMetadata.getColumns(vtName);
                    if (vtCols == null) continue;
                    int vtOrdinal = 1;
                    for (Object[] colDef : vtCols) {
                        String colName = (String) colDef[0];
                        if (!matchesPattern(colName, columnNamePattern)) {
                            vtOrdinal++;
                            continue;
                        }
                        int jdbcType = (int) colDef[1];
                        String typeName = (String) colDef[2];
                        int displaySize = (int) colDef[3];

                        rows.add(Arrays.asList(
                                vtCatalog, VirtualTableMetadata.SCHEMA_NAME, vtName, colName,
                                jdbcType, typeName, displaySize, null,
                                0, 10, columnNullable, "",
                                null, 0, 0, displaySize,
                                vtOrdinal, "YES", null, null, null, null, "NO", "NO"
                        ));
                        vtOrdinal++;
                    }
                }
            }
        }

        // Real columns from Snowflake via SHOW COLUMNS (skip if schema is exclusively _keboola)
        boolean schemaIsOnlyVirtual = VirtualTableMetadata.SCHEMA_NAME.equalsIgnoreCase(schemaPattern);
        if (!schemaIsOnlyVirtual) {
        StringBuilder sql = new StringBuilder("SHOW COLUMNS");
        if (columnNamePattern != null && !columnNamePattern.isEmpty() && !columnNamePattern.equals("%")) {
            sql.append(" LIKE '").append(escapeSingleQuoteForLike(columnNamePattern)).append("'");
        }
        sql.append(buildColumnsInClause(catalog, schemaPattern, tableNamePattern));

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql.toString())) {
            int dbIdx = findColumnIndex(rs, "database_name");
            int schemaIdx = findColumnIndex(rs, "schema_name");
            int tableIdx = findColumnIndex(rs, "table_name");
            int colNameIdx = findColumnIndex(rs, "column_name");
            int dataTypeIdx = findColumnIndex(rs, "data_type");

            String lastTableKey = null;
            int ordinal = 0;

            while (rs.next()) {
                String dbName = rs.getString(dbIdx);
                String schemaName = rs.getString(schemaIdx);
                String tableName = rs.getString(tableIdx);
                String colName = rs.getString(colNameIdx);
                String dataTypeJson = rs.getString(dataTypeIdx);

                if (isFilteredDatabase(dbName)) continue;
                if (!matchesPattern(schemaName, schemaPattern)) continue;
                if (!matchesPattern(tableName, tableNamePattern)) continue;
                if (!matchesPattern(dbName, catalog)) continue;

                String tableKey = dbName + "." + schemaName + "." + tableName;
                if (!tableKey.equals(lastTableKey)) {
                    ordinal = 0;
                    lastTableKey = tableKey;
                }
                ordinal++;

                String sfType = "VARCHAR";
                int precision = 0;
                int scale = 0;
                int length = 0;
                boolean nullable = true;

                try {
                    JsonNode node = OBJECT_MAPPER.readTree(dataTypeJson);
                    sfType = node.has("type") ? node.get("type").asText("VARCHAR") : "VARCHAR";
                    precision = node.has("precision") ? node.get("precision").asInt(0) : 0;
                    scale = node.has("scale") ? node.get("scale").asInt(0) : 0;
                    length = node.has("length") ? node.get("length").asInt(0) : 0;
                    nullable = !node.has("nullable") || node.get("nullable").asBoolean(true);
                } catch (Exception e) {
                    LOG.warn("Failed to parse data_type JSON for column {}.{}.{}.{}: {}",
                            dbName, schemaName, tableName, colName, e.getMessage());
                }

                int jdbcType = TypeMapper.toJdbcType(sfType);
                String typeName = TypeMapper.toJdbcTypeName(sfType);

                int columnSize;
                int decimalDigits;
                if (precision > 0) {
                    columnSize = precision;
                    decimalDigits = scale;
                } else if (length > 0) {
                    columnSize = length;
                    decimalDigits = 0;
                } else {
                    columnSize = TypeMapper.getDisplaySize(sfType, null);
                    decimalDigits = TypeMapper.getScale(sfType);
                }

                rows.add(Arrays.asList(
                        dbName, schemaName, tableName, colName,
                        jdbcType, typeName, columnSize, null,
                        decimalDigits, 10, nullable ? columnNullable : columnNoNulls, "",
                        null, 0, 0, columnSize,
                        ordinal, nullable ? "YES" : "NO",
                        null, null, null, null, "NO", "NO"
                ));
            }
        }
        } // end if !schemaIsOnlyVirtual

        return new ArrayResultSet(columns, rows);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        List<String> columns = Collections.singletonList("TABLE_TYPE");
        List<List<Object>> rows = new ArrayList<>();
        rows.add(Collections.singletonList("TABLE"));
        rows.add(Collections.singletonList("VIEW"));
        rows.add(Collections.singletonList("VIRTUAL TABLE"));
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
    public String getIdentifierQuoteString() { return "\""; }

    @Override
    public String getCatalogSeparator() { return "."; }

    @Override
    public String getCatalogTerm() { return "database"; }

    @Override
    public String getSchemaTerm() { return "schema"; }

    @Override
    public String getProcedureTerm() { return "procedure"; }

    @Override
    public String getSearchStringEscape() { return "\\"; }

    @Override
    public String getExtraNameCharacters() { return ""; }

    @Override
    public String getSQLKeywords() { return ""; }

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

    @Override public boolean supportsAlterTableWithAddColumn() { return false; }
    @Override public boolean supportsAlterTableWithDropColumn() { return false; }
    @Override public boolean supportsColumnAliasing() { return true; }
    @Override public boolean supportsGroupBy() { return true; }
    @Override public boolean supportsGroupByUnrelated() { return true; }
    @Override public boolean supportsGroupByBeyondSelect() { return true; }
    @Override public boolean supportsOrderByUnrelated() { return true; }
    @Override public boolean supportsSelectForUpdate() { return false; }
    @Override public boolean supportsSubqueriesInComparisons() { return true; }
    @Override public boolean supportsSubqueriesInExists() { return true; }
    @Override public boolean supportsSubqueriesInIns() { return true; }
    @Override public boolean supportsSubqueriesInQuantifieds() { return true; }
    @Override public boolean supportsCorrelatedSubqueries() { return true; }
    @Override public boolean supportsUnion() { return true; }
    @Override public boolean supportsUnionAll() { return true; }
    @Override public boolean supportsOuterJoins() { return true; }
    @Override public boolean supportsFullOuterJoins() { return true; }
    @Override public boolean supportsLimitedOuterJoins() { return true; }
    @Override public boolean supportsExpressionsInOrderBy() { return true; }
    @Override public boolean supportsLikeEscapeClause() { return true; }
    @Override public boolean supportsMultipleResultSets() { return false; }
    @Override public boolean supportsMultipleTransactions() { return false; }
    @Override public boolean supportsNonNullableColumns() { return true; }
    @Override public boolean supportsMinimumSQLGrammar() { return true; }
    @Override public boolean supportsCoreSQLGrammar() { return true; }
    @Override public boolean supportsExtendedSQLGrammar() { return false; }
    @Override public boolean supportsANSI92EntryLevelSQL() { return true; }
    @Override public boolean supportsANSI92IntermediateSQL() { return false; }
    @Override public boolean supportsANSI92FullSQL() { return false; }
    @Override public boolean supportsIntegrityEnhancementFacility() { return false; }
    @Override public boolean supportsMixedCaseIdentifiers() { return false; }
    @Override public boolean storesUpperCaseIdentifiers() { return true; }
    @Override public boolean storesLowerCaseIdentifiers() { return false; }
    @Override public boolean storesMixedCaseIdentifiers() { return false; }
    @Override public boolean supportsMixedCaseQuotedIdentifiers() { return true; }
    @Override public boolean storesUpperCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesLowerCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesMixedCaseQuotedIdentifiers() { return true; }
    @Override public boolean supportsTableCorrelationNames() { return true; }
    @Override public boolean supportsDifferentTableCorrelationNames() { return false; }
    @Override public boolean supportsTransactions() { return false; }
    @Override public boolean supportsTransactionIsolationLevel(int level) { return false; }
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() { return false; }
    @Override public boolean supportsDataManipulationTransactionsOnly() { return false; }
    @Override public boolean dataDefinitionCausesTransactionCommit() { return false; }
    @Override public boolean dataDefinitionIgnoredInTransactions() { return false; }
    @Override public boolean supportsSchemasInDataManipulation() { return true; }
    @Override public boolean supportsSchemasInProcedureCalls() { return false; }
    @Override public boolean supportsSchemasInTableDefinitions() { return true; }
    @Override public boolean supportsSchemasInIndexDefinitions() { return false; }
    @Override public boolean supportsSchemasInPrivilegeDefinitions() { return false; }
    @Override public boolean supportsCatalogsInDataManipulation() { return true; }
    @Override public boolean supportsCatalogsInProcedureCalls() { return false; }
    @Override public boolean supportsCatalogsInTableDefinitions() { return true; }
    @Override public boolean supportsCatalogsInIndexDefinitions() { return false; }
    @Override public boolean supportsCatalogsInPrivilegeDefinitions() { return false; }
    @Override public boolean supportsPositionedDelete() { return false; }
    @Override public boolean supportsPositionedUpdate() { return false; }
    @Override public boolean supportsStoredProcedures() { return false; }
    @Override public boolean supportsBatchUpdates() { return true; }
    @Override public boolean supportsSavepoints() { return false; }
    @Override public boolean supportsNamedParameters() { return false; }
    @Override public boolean supportsMultipleOpenResults() { return false; }
    @Override public boolean supportsGetGeneratedKeys() { return false; }

    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override public boolean supportsResultSetHoldability(int holdability) { return false; }
    @Override public int getResultSetHoldability() { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }
    @Override public boolean supportsStatementPooling() { return false; }
    @Override public boolean supportsStoredFunctionsUsingCallSyntax() { return false; }
    @Override public boolean autoCommitFailureClosesAllResultSets() { return false; }
    @Override public boolean generatedKeyAlwaysReturned() { return false; }

    // -- Limits --

    @Override public int getMaxBinaryLiteralLength() { return 0; }
    @Override public int getMaxCharLiteralLength() { return 0; }
    @Override public int getMaxColumnNameLength() { return 255; }
    @Override public int getMaxColumnsInGroupBy() { return 0; }
    @Override public int getMaxColumnsInIndex() { return 0; }
    @Override public int getMaxColumnsInOrderBy() { return 0; }
    @Override public int getMaxColumnsInSelect() { return 0; }
    @Override public int getMaxColumnsInTable() { return 0; }
    @Override public int getMaxConnections() { return 0; }
    @Override public int getMaxCursorNameLength() { return 0; }
    @Override public int getMaxIndexLength() { return 0; }
    @Override public int getMaxSchemaNameLength() { return 255; }
    @Override public int getMaxProcedureNameLength() { return 0; }
    @Override public int getMaxCatalogNameLength() { return 255; }
    @Override public int getMaxRowSize() { return 0; }
    @Override public boolean doesMaxRowSizeIncludeBlobs() { return false; }
    @Override public int getMaxStatementLength() { return 0; }
    @Override public int getMaxStatements() { return 0; }
    @Override public int getMaxTableNameLength() { return 255; }
    @Override public int getMaxTablesInSelect() { return 0; }
    @Override public int getMaxUserNameLength() { return 0; }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    // -- Other required methods --

    @Override public boolean isReadOnly() { return true; }
    @Override public boolean nullsAreSortedHigh() { return true; }
    @Override public boolean nullsAreSortedLow() { return false; }
    @Override public boolean nullsAreSortedAtStart() { return false; }
    @Override public boolean nullsAreSortedAtEnd() { return false; }
    @Override public boolean usesLocalFiles() { return false; }
    @Override public boolean usesLocalFilePerTable() { return false; }
    @Override public boolean nullPlusNonNullIsNull() { return true; }
    @Override public boolean supportsConvert() { return false; }
    @Override public boolean supportsConvert(int fromType, int toType) { return false; }
    @Override public boolean allProceduresAreCallable() { return false; }
    @Override public boolean allTablesAreSelectable() { return true; }

    @Override
    public String getURL() {
        return "jdbc:keboola://" + connection.getHost();
    }

    @Override
    public String getUserName() {
        return connection.getTokenInfo() != null ? connection.getTokenInfo().getDescription() : null;
    }

    @Override public boolean isCatalogAtStart() { return true; }
    @Override public boolean ownUpdatesAreVisible(int type) { return false; }
    @Override public boolean ownDeletesAreVisible(int type) { return false; }
    @Override public boolean ownInsertsAreVisible(int type) { return false; }
    @Override public boolean othersUpdatesAreVisible(int type) { return false; }
    @Override public boolean othersDeletesAreVisible(int type) { return false; }
    @Override public boolean othersInsertsAreVisible(int type) { return false; }
    @Override public boolean updatesAreDetected(int type) { return false; }
    @Override public boolean deletesAreDetected(int type) { return false; }
    @Override public boolean insertsAreDetected(int type) { return false; }
    @Override public boolean locatorsUpdateCopy() { return false; }
    @Override public RowIdLifetime getRowIdLifetime() { return RowIdLifetime.ROWID_UNSUPPORTED; }
    @Override public int getSQLStateType() { return sqlStateSQL; }

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
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }

    // -- Helpers --

    private boolean isFilteredDatabase(String dbName) {
        return dbName != null && DriverConfig.FILTERED_DATABASES.contains(dbName.toUpperCase());
    }

    private String escapeSingleQuoteForLike(String pattern) {
        return pattern.replace("'", "''");
    }

    private boolean matchesPattern(String value, String sqlPattern) {
        if (sqlPattern == null || sqlPattern.equals("%")) return true;
        if (value == null) return false;
        if (sqlPattern.isEmpty()) return value.isEmpty();
        String regex = sqlPattern
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

    private String buildCatalogInClause(String catalog) {
        if (catalog != null && !catalog.equals("%")) {
            return " IN DATABASE \"" + catalog.replace("\"", "\"\"") + "\"";
        }
        return "";
    }

    private String buildScopeInClause(String catalog, String schema) {
        boolean hasCatalog = catalog != null && !catalog.isEmpty() && !catalog.equals("%");
        boolean hasSchema = schema != null && !schema.isEmpty() && !schema.equals("%");

        if (hasCatalog && hasSchema) {
            return " IN SCHEMA \"" + catalog.replace("\"", "\"\"") + "\".\"" + schema.replace("\"", "\"\"") + "\"";
        } else if (hasCatalog) {
            return " IN DATABASE \"" + catalog.replace("\"", "\"\"") + "\"";
        }
        return "";
    }

    private String buildColumnsInClause(String catalog, String schema, String table) {
        boolean hasCatalog = catalog != null && !catalog.isEmpty() && !catalog.equals("%");
        boolean hasSchema = schema != null && !schema.isEmpty() && !schema.equals("%");
        boolean hasTable = table != null && !table.isEmpty() && !table.equals("%");

        if (hasCatalog && hasSchema && hasTable) {
            return " IN TABLE \"" + catalog.replace("\"", "\"\"") + "\".\"" +
                    schema.replace("\"", "\"\"") + "\".\"" + table.replace("\"", "\"\"") + "\"";
        } else if (hasCatalog && hasSchema) {
            return " IN SCHEMA \"" + catalog.replace("\"", "\"\"") + "\".\"" + schema.replace("\"", "\"\"") + "\"";
        } else if (hasCatalog) {
            return " IN DATABASE \"" + catalog.replace("\"", "\"\"") + "\"";
        }
        return "";
    }

    private int findColumnIndex(ResultSet rs, String columnName) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (meta.getColumnName(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        throw new SQLException("Column '" + columnName + "' not found in result");
    }

    @Override public boolean supportsOpenStatementsAcrossRollback() throws SQLException { return false; }
    @Override public boolean supportsOpenStatementsAcrossCommit() throws SQLException { return false; }
    @Override public boolean supportsOpenCursorsAcrossRollback() throws SQLException { return false; }
    @Override public boolean supportsOpenCursorsAcrossCommit() throws SQLException { return false; }

    private ResultSet emptyKeysResultSet() throws SQLException {
        return new ArrayResultSet(
                Arrays.asList("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
                        "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
                        "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"),
                Collections.emptyList()
        );
    }
}
