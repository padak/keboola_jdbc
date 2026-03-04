package com.keboola.jdbc;

import com.keboola.jdbc.config.ConnectionConfig;
import com.keboola.jdbc.exception.KeboolaJdbcException;
import com.keboola.jdbc.http.QueryServiceClient;
import com.keboola.jdbc.http.StorageApiClient;
import com.keboola.jdbc.http.model.Branch;
import com.keboola.jdbc.http.model.TokenInfo;
import com.keboola.jdbc.http.model.Workspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * JDBC Connection implementation for Keboola.
 *
 * <p>On construction the following setup is performed:
 * <ol>
 *   <li>Create a {@link StorageApiClient} and verify the provided token</li>
 *   <li>Discover the Query Service URL via the Storage API</li>
 *   <li>Resolve the branch ID (from config or by finding the default branch)</li>
 *   <li>Resolve the workspace ID (required - must be provided in config)</li>
 *   <li>Create a {@link QueryServiceClient} for subsequent query execution</li>
 * </ol>
 *
 * <p>All connections are auto-commit and read-only by default (no transaction support).
 */
public class KeboolaConnection implements Connection {

    private static final Logger LOG = LoggerFactory.getLogger(KeboolaConnection.class);

    private final String host;
    private final StorageApiClient storageClient;
    private final QueryServiceClient queryClient;
    private final TokenInfo tokenInfo;
    private final long branchId;
    private final long workspaceId;

    private boolean closed = false;
    private boolean autoCommit = true;
    private boolean readOnly = true;
    private String catalog;
    private String currentSchema;

    /**
     * Establishes a connection to Keboola using the provided configuration.
     *
     * @param config the parsed connection configuration (host, token, optional branch/workspace)
     * @throws SQLException if token verification, service discovery, or branch/workspace resolution fails
     */
    public KeboolaConnection(ConnectionConfig config) throws SQLException {
        LOG.info("Connecting to Keboola: host={}", config.getHost());

        try {
            this.host = config.getHost();
            storageClient = new StorageApiClient(config.getHost(), config.getToken());
            tokenInfo = verifyToken();
            String queryServiceUrl = discoverQueryServiceUrl();
            queryClient = new QueryServiceClient(queryServiceUrl, config.getToken());
            branchId = resolveBranchId(config);
            workspaceId = resolveWorkspaceId(config);
            catalog = tokenInfo.getOwner().getName();

            LOG.info("Connected: project='{}', branchId={}, workspaceId={}",
                    catalog, branchId, workspaceId);

        } catch (KeboolaJdbcException e) {
            throw new SQLException("Failed to connect to Keboola: " + e.getMessage(), e);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Unexpected error connecting to Keboola: " + e.getMessage(), e);
        }
    }

    // -------------------------------------------------------------------------
    // Connection setup helpers
    // -------------------------------------------------------------------------

    /**
     * Verifies the Storage API token and returns the associated token info.
     *
     * @return token info with project and owner details
     * @throws KeboolaJdbcException if the token is invalid
     */
    private TokenInfo verifyToken() throws KeboolaJdbcException {
        LOG.debug("Verifying Storage API token");
        TokenInfo info = storageClient.verifyToken();
        LOG.debug("Token verified: project='{}', tokenId={}", info.getOwner().getName(), info.getId());
        return info;
    }

    /**
     * Discovers the Query Service URL from the Storage API.
     *
     * @return the Query Service base URL
     * @throws KeboolaJdbcException if discovery fails
     */
    private String discoverQueryServiceUrl() throws KeboolaJdbcException {
        LOG.debug("Discovering Query Service URL");
        String url = storageClient.discoverQueryServiceUrl();
        LOG.debug("Query Service URL: {}", url);
        return url;
    }

    /**
     * Resolves the branch ID to use for queries.
     *
     * <p>If a branch is specified in the config (by ID or name), it is used directly or looked up.
     * Otherwise, the default branch is found from the Storage API.
     *
     * @param config the connection configuration
     * @return the resolved branch ID
     * @throws KeboolaJdbcException if branch resolution fails
     */
    private long resolveBranchId(ConnectionConfig config) throws KeboolaJdbcException {
        Long brId = config.getBranchId();
        if (brId != null && brId > 0) {
            LOG.debug("Using configured branchId={}", brId);
            return brId;
        }

        LOG.debug("Resolving default branch");
        List<Branch> branches = storageClient.listBranches();
        return branches.stream()
                .filter(Branch::isDefault)
                .findFirst()
                .map(Branch::getId)
                .orElseThrow(() -> new KeboolaJdbcException("No default branch found in the project"));
    }

    /**
     * Resolves the workspace ID. If not specified in config, auto-discovers from API:
     * - Lists all active workspaces
     * - If exactly one active workspace exists, uses it automatically
     * - If multiple exist, lists them in the error message so the user can pick one
     * - If none exist, suggests creating one
     *
     * @param config the connection configuration
     * @return the resolved workspace ID
     * @throws KeboolaJdbcException if workspace cannot be resolved
     */
    private long resolveWorkspaceId(ConnectionConfig config) throws KeboolaJdbcException {
        Long wsId = config.getWorkspaceId();
        if (wsId != null && wsId > 0) {
            LOG.debug("Using configured workspaceId={}", wsId);
            return wsId;
        }

        LOG.debug("No workspace specified, auto-discovering from API");
        List<Workspace> allWorkspaces = storageClient.listWorkspaces();
        // Storage API does not return a "status" field for workspaces - if they are listed, they exist.
        // Prefer the most recently created workspace (last in list, highest ID).
        List<Workspace> activeWorkspaces = allWorkspaces;

        if (activeWorkspaces.isEmpty()) {
            throw new KeboolaJdbcException(
                    "No active workspaces found in the project. "
                    + "Create a workspace in Keboola UI first, then reconnect.");
        }

        // Auto-select the most recently created workspace (last in list = highest ID)
        Workspace selected = activeWorkspaces.get(activeWorkspaces.size() - 1);
        LOG.info("Auto-selected workspace: id={}, name='{}', type={} (out of {} available)",
                selected.getId(), selected.getName(), selected.getType(), activeWorkspaces.size());
        return selected.getId();
    }

    // -------------------------------------------------------------------------
    // Statement factory methods
    // -------------------------------------------------------------------------

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new KeboolaStatement(this, queryClient, branchId, workspaceId);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        validateResultSetType(resultSetType, resultSetConcurrency);
        return createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        checkClosed();
        validateResultSetType(resultSetType, resultSetConcurrency);
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new KeboolaPreparedStatement(this, queryClient, branchId, workspaceId, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        checkClosed();
        validateResultSetType(resultSetType, resultSetConcurrency);
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        checkClosed();
        validateResultSetType(resultSetType, resultSetConcurrency);
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall (stored procedures) not supported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall (stored procedures) not supported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall (stored procedures) not supported");
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new KeboolaDatabaseMetaData(this, storageClient, tokenInfo);
    }

    // -------------------------------------------------------------------------
    // Connection lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
            LOG.info("KeboolaConnection closed");
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return !closed;
    }

    // -------------------------------------------------------------------------
    // Catalog / schema
    // -------------------------------------------------------------------------

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return catalog;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        // no-op: catalog is determined by the token / project
    }

    @Override
    public String getSchema() throws SQLException {
        return currentSchema;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        LOG.debug("setSchema({})", schema);
        this.currentSchema = schema;
    }

    // -------------------------------------------------------------------------
    // Auto-commit (always true - no transaction support)
    // -------------------------------------------------------------------------

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        // stored but always effectively true - Keboola does not support transactions
        this.autoCommit = autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        // no-op: auto-commit mode
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        // no-op: auto-commit mode
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    // -------------------------------------------------------------------------
    // Read-only mode
    // -------------------------------------------------------------------------

    @Override
    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }

    // -------------------------------------------------------------------------
    // Transaction isolation (reported as READ_COMMITTED, no actual support)
    // -------------------------------------------------------------------------

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        // no-op: transactions are not supported
    }

    // -------------------------------------------------------------------------
    // Warnings
    // -------------------------------------------------------------------------

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // no-op
    }

    // -------------------------------------------------------------------------
    // SQL escaping
    // -------------------------------------------------------------------------

    @Override
    public String nativeSQL(String sql) throws SQLException {
        // Return as-is: no SQL translation needed
        return sql;
    }

    // -------------------------------------------------------------------------
    // Type map
    // -------------------------------------------------------------------------

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("getTypeMap not supported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("setTypeMap not supported");
    }

    // -------------------------------------------------------------------------
    // Holdability
    // -------------------------------------------------------------------------

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        // no-op
    }

    // -------------------------------------------------------------------------
    // Savepoints (not supported)
    // -------------------------------------------------------------------------

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    // -------------------------------------------------------------------------
    // BLOB / CLOB / NCLOB / SQLXML / Array / Struct factories
    // -------------------------------------------------------------------------

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createBlob not supported");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createClob not supported");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createNClob not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("createSQLXML not supported");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("createArrayOf not supported");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStruct not supported");
    }

    // -------------------------------------------------------------------------
    // Client info
    // -------------------------------------------------------------------------

    @Override
    public Properties getClientInfo() throws SQLException {
        return new Properties();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // no-op
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // no-op
    }

    // -------------------------------------------------------------------------
    // Abort / network timeout
    // -------------------------------------------------------------------------

    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        // no-op
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
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
    // Accessors for internal use by other driver classes
    // -------------------------------------------------------------------------

    /** Returns the Keboola Connection host, e.g. "connection.keboola.com". */
    public String getHost() {
        return host;
    }

    /** Returns the Storage API client (used by KeboolaDatabaseMetaData). */
    public StorageApiClient getStorageClient() {
        return storageClient;
    }

    /** Returns the Query Service client (used by KeboolaStatement). */
    public QueryServiceClient getQueryClient() {
        return queryClient;
    }

    /** Returns the verified token info (project name, owner, etc.). */
    public TokenInfo getTokenInfo() {
        return tokenInfo;
    }

    /** Returns the resolved branch ID used for all query executions on this connection. */
    public long getBranchId() {
        return branchId;
    }

    /** Returns the resolved workspace ID used for all query executions on this connection. */
    public long getWorkspaceId() {
        return workspaceId;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }

    /**
     * Validates that the requested ResultSet type and concurrency are supported.
     *
     * @param resultSetType        must be TYPE_FORWARD_ONLY
     * @param resultSetConcurrency must be CONCUR_READ_ONLY
     * @throws SQLFeatureNotSupportedException if unsupported values are requested
     */
    private void validateResultSetType(int resultSetType, int resultSetConcurrency)
            throws SQLFeatureNotSupportedException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException(
                    "Only TYPE_FORWARD_ONLY ResultSet type is supported, got: " + resultSetType);
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException(
                    "Only CONCUR_READ_ONLY ResultSet concurrency is supported, got: " + resultSetConcurrency);
        }
    }
}
