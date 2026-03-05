package com.keboola.jdbc;

import com.keboola.jdbc.command.VirtualTableMetadata;
import com.keboola.jdbc.config.DriverConfig;
import com.keboola.jdbc.http.StorageApiClient;
import com.keboola.jdbc.http.model.Bucket;
import com.keboola.jdbc.http.model.ColumnMetadata;
import com.keboola.jdbc.http.model.TableInfo;
import com.keboola.jdbc.http.model.TokenInfo;
import com.keboola.jdbc.meta.SchemaCache;
import com.keboola.jdbc.meta.TypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DatabaseMetaData implementation for Keboola.
 * Maps Keboola Storage structure to JDBC metadata:
 * - Catalog = Project (from token owner)
 * - Schema = Bucket (e.g. "in.c-main")
 * - Table = Table within a bucket
 * - Columns = Table columns with type info from KBC.datatype.* metadata
 */
public class KeboolaDatabaseMetaData implements DatabaseMetaData {

    private static final Logger LOG = LoggerFactory.getLogger(KeboolaDatabaseMetaData.class);

    private final KeboolaConnection connection;
    private final TokenInfo tokenInfo;
    private final SchemaCache schemaCache;

    public KeboolaDatabaseMetaData(KeboolaConnection connection, StorageApiClient storageClient, TokenInfo tokenInfo) {
        this.connection = connection;
        this.tokenInfo = tokenInfo;
        this.schemaCache = new SchemaCache(storageClient);
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
        String projectName = tokenInfo.getOwner() != null ? tokenInfo.getOwner().getName() : "Keboola";
        rows.add(Collections.singletonList(projectName));
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
        String projectName = tokenInfo.getOwner() != null ? tokenInfo.getOwner().getName() : "Keboola";

        // Add virtual _keboola schema
        if (matchesPattern(VirtualTableMetadata.SCHEMA_NAME, schemaPattern)) {
            rows.add(Arrays.asList(VirtualTableMetadata.SCHEMA_NAME, projectName));
        }

        for (Bucket bucket : schemaCache.getBuckets()) {
            if (matchesPattern(bucket.getId(), schemaPattern)) {
                rows.add(Arrays.asList(bucket.getId(), projectName));
            }
        }

        // Sort by schema name
        rows.sort(Comparator.comparing(r -> (String) r.get(0)));
        return new ArrayResultSet(columns, rows);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        LOG.debug("getTables(catalog={}, schema={}, table={}, types={})", catalog, schemaPattern, tableNamePattern, types != null ? Arrays.toString(types) : "null");

        // Determine which types to include
        boolean includeTable = true;
        boolean includeVirtual = true;
        if (types != null) {
            includeTable = false;
            includeVirtual = false;
            for (String type : types) {
                if ("TABLE".equalsIgnoreCase(type)) {
                    includeTable = true;
                }
                if ("VIRTUAL TABLE".equalsIgnoreCase(type)) {
                    includeVirtual = true;
                }
            }
            if (!includeTable && !includeVirtual) {
                return emptyTableResultSet();
            }
        }

        List<String> columns = Arrays.asList(
                "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
                "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"
        );
        List<List<Object>> rows = new ArrayList<>();
        String projectName = tokenInfo.getOwner() != null ? tokenInfo.getOwner().getName() : "Keboola";

        // Add virtual _keboola tables
        if (includeVirtual && matchesPattern(VirtualTableMetadata.SCHEMA_NAME, schemaPattern)) {
            for (String vtName : VirtualTableMetadata.getTableNames()) {
                if (matchesPattern(vtName, tableNamePattern)) {
                    rows.add(Arrays.asList(
                            projectName,                          // TABLE_CAT
                            VirtualTableMetadata.SCHEMA_NAME,     // TABLE_SCHEM
                            vtName,                               // TABLE_NAME
                            "VIRTUAL TABLE",                      // TABLE_TYPE
                            "Keboola platform metadata",          // REMARKS
                            null, null, null, null, null
                    ));
                }
            }
        }

        // Add real Storage API tables
        if (includeTable) {
            for (Bucket bucket : schemaCache.getBuckets()) {
                if (!matchesPattern(bucket.getId(), schemaPattern)) {
                    continue;
                }
                for (TableInfo table : schemaCache.getTables(bucket.getId())) {
                    if (matchesPattern(table.getName(), tableNamePattern)) {
                        rows.add(Arrays.asList(
                                projectName,           // TABLE_CAT
                                bucket.getId(),         // TABLE_SCHEM
                                table.getName(),        // TABLE_NAME
                                "TABLE",                // TABLE_TYPE
                                "",                     // REMARKS
                                null, null, null, null, null
                        ));
                    }
                }
            }
        }

        rows.sort((a, b) -> {
            int cmp = ((String) a.get(1)).compareTo((String) b.get(1));
            if (cmp != 0) return cmp;
            return ((String) a.get(2)).compareTo((String) b.get(2));
        });

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
        List<List<Object>> rows = new ArrayList<>();
        String projectName = tokenInfo.getOwner() != null ? tokenInfo.getOwner().getName() : "Keboola";

        // Add columns for virtual _keboola tables
        if (matchesPattern(VirtualTableMetadata.SCHEMA_NAME, schemaPattern)) {
            for (String vtName : VirtualTableMetadata.getTableNames()) {
                if (!matchesPattern(vtName, tableNamePattern)) {
                    continue;
                }
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
                            projectName,                          // TABLE_CAT
                            VirtualTableMetadata.SCHEMA_NAME,     // TABLE_SCHEM
                            vtName,                               // TABLE_NAME
                            colName,                              // COLUMN_NAME
                            jdbcType,                             // DATA_TYPE
                            typeName,                             // TYPE_NAME
                            displaySize,                          // COLUMN_SIZE
                            null,                                 // BUFFER_LENGTH
                            0,                                    // DECIMAL_DIGITS
                            10,                                   // NUM_PREC_RADIX
                            columnNullable,                       // NULLABLE
                            "",                                   // REMARKS
                            null,                                 // COLUMN_DEF
                            0,                                    // SQL_DATA_TYPE
                            0,                                    // SQL_DATETIME_SUB
                            displaySize,                          // CHAR_OCTET_LENGTH
                            vtOrdinal,                            // ORDINAL_POSITION
                            "YES",                                // IS_NULLABLE
                            null, null, null, null,               // SCOPE_*
                            "NO",                                 // IS_AUTOINCREMENT
                            "NO"                                  // IS_GENERATEDCOLUMN
                    ));
                    vtOrdinal++;
                }
            }
        }

        // Add columns for real Storage API tables
        for (Bucket bucket : schemaCache.getBuckets()) {
            if (!matchesPattern(bucket.getId(), schemaPattern)) {
                continue;
            }
            for (TableInfo table : schemaCache.getTables(bucket.getId())) {
                if (!matchesPattern(table.getName(), tableNamePattern)) {
                    continue;
                }
                if (table.getColumns() == null) {
                    continue;
                }
                int ordinal = 1;
                for (com.keboola.jdbc.http.model.ColumnInfo col : table.getColumns()) {
                    String colName = col.getName();
                    if (!matchesPattern(colName, columnNamePattern)) {
                        ordinal++;
                        continue;
                    }

                    // Extract type metadata from columnMetadata
                    String dataType = "VARCHAR";
                    boolean nullable = true;
                    Integer length = null;

                    Map<String, List<ColumnMetadata>> colMeta = table.getColumnMetadata();
                    if (colMeta != null && colMeta.containsKey(colName)) {
                        for (ColumnMetadata meta : colMeta.get(colName)) {
                            switch (meta.getKey()) {
                                case "KBC.datatype.type":
                                    dataType = meta.getValue();
                                    break;
                                case "KBC.datatype.nullable":
                                    nullable = "1".equals(meta.getValue()) || "true".equalsIgnoreCase(meta.getValue());
                                    break;
                                case "KBC.datatype.length":
                                    try {
                                        length = Integer.parseInt(meta.getValue());
                                    } catch (NumberFormatException ignored) {
                                        // Keep null
                                    }
                                    break;
                            }
                        }
                    }

                    int jdbcType = TypeMapper.toJdbcType(dataType);
                    String typeName = TypeMapper.toJdbcTypeName(dataType);
                    int displaySize = TypeMapper.getDisplaySize(dataType, length);
                    int precision = TypeMapper.getPrecision(dataType, length);
                    int scale = TypeMapper.getScale(dataType);

                    rows.add(Arrays.asList(
                            projectName,                        // TABLE_CAT
                            bucket.getId(),                     // TABLE_SCHEM
                            table.getName(),                    // TABLE_NAME
                            colName,                            // COLUMN_NAME
                            jdbcType,                           // DATA_TYPE
                            typeName,                           // TYPE_NAME
                            displaySize,                        // COLUMN_SIZE
                            null,                               // BUFFER_LENGTH
                            scale,                              // DECIMAL_DIGITS
                            10,                                 // NUM_PREC_RADIX
                            nullable ? columnNullable : columnNoNulls, // NULLABLE
                            "",                                 // REMARKS
                            null,                               // COLUMN_DEF
                            0,                                  // SQL_DATA_TYPE
                            0,                                  // SQL_DATETIME_SUB
                            displaySize,                        // CHAR_OCTET_LENGTH
                            ordinal,                            // ORDINAL_POSITION
                            nullable ? "YES" : "NO",            // IS_NULLABLE
                            null, null, null, null,             // SCOPE_*
                            "NO",                               // IS_AUTOINCREMENT
                            "NO"                                // IS_GENERATEDCOLUMN
                    ));
                    ordinal++;
                }
            }
        }

        return new ArrayResultSet(columns, rows);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        List<String> columns = Collections.singletonList("TABLE_TYPE");
        List<List<Object>> rows = new ArrayList<>();
        rows.add(Collections.singletonList("TABLE"));
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
    public String getIdentifierQuoteString() {
        return "\"";
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public String getCatalogTerm() {
        return "project";
    }

    @Override
    public String getSchemaTerm() {
        return "bucket";
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
    public boolean supportsCatalogsInDataManipulation() { return false; }

    @Override
    public boolean supportsCatalogsInProcedureCalls() { return false; }

    @Override
    public boolean supportsCatalogsInTableDefinitions() { return false; }

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
        return tokenInfo.getDescription();
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
        // Convert SQL LIKE pattern to regex
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
