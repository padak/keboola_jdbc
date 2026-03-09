package com.keboola.jdbc;

import com.keboola.jdbc.http.QueryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC PreparedStatement implementation for Keboola.
 *
 * <p>Stores the SQL template with {@code ?} placeholders and a map of parameter values.
 * When executing, replaces placeholders sequentially with properly escaped SQL literals,
 * then delegates to the parent {@link KeboolaStatement} execution logic.
 *
 * <p>Parameter indices are 1-based as per the JDBC specification.
 */
public class KeboolaPreparedStatement extends KeboolaStatement implements PreparedStatement {

    private static final Logger LOG = LoggerFactory.getLogger(KeboolaPreparedStatement.class);

    /** The SQL template containing {@code ?} placeholders. */
    private final String sqlTemplate;

    /**
     * Parameter values keyed by 1-based parameter index.
     * Values are already formatted as SQL literals (quoted strings, numbers, NULL, etc.).
     */
    private final Map<Integer, String> params = new HashMap<>();

    /**
     * SQL statements accumulated via {@link #addBatch()}.
     * Each entry is the fully-resolved SQL with parameters substituted.
     */
    private final List<String> batchSqls = new ArrayList<>();

    /**
     * Creates a new PreparedStatement for the given SQL template.
     *
     * @param connection   the parent connection
     * @param queryClient  HTTP client for the Keboola Query Service
     * @param branchId     the branch ID to execute queries against
     * @param workspaceId  the workspace ID to execute queries against
     * @param sql          the SQL template with {@code ?} placeholders
     */
    public KeboolaPreparedStatement(KeboolaConnection connection,
                                    QueryServiceClient queryClient,
                                    long branchId,
                                    long workspaceId,
                                    String sql) {
        super(connection, queryClient, branchId, workspaceId);
        this.sqlTemplate = sql;
        LOG.debug("KeboolaPreparedStatement created: sql={}", sql);
    }

    // -------------------------------------------------------------------------
    // PreparedStatement execute methods
    // -------------------------------------------------------------------------

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(resolveParameters());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return executeUpdate(resolveParameters());
    }

    @Override
    public boolean execute() throws SQLException {
        return execute(resolveParameters());
    }

    // -------------------------------------------------------------------------
    // Parameter setters
    // -------------------------------------------------------------------------

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkParameterIndex(parameterIndex);
        params.put(parameterIndex, "NULL");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkParameterIndex(parameterIndex);
        params.put(parameterIndex, x ? "TRUE" : "FALSE");
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkParameterIndex(parameterIndex);
        params.put(parameterIndex, Byte.toString(x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkParameterIndex(parameterIndex);
        params.put(parameterIndex, Short.toString(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkParameterIndex(parameterIndex);
        params.put(parameterIndex, Integer.toString(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkParameterIndex(parameterIndex);
        params.put(parameterIndex, Long.toString(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkParameterIndex(parameterIndex);
        params.put(parameterIndex, Float.toString(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkParameterIndex(parameterIndex);
        params.put(parameterIndex, Double.toString(x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkParameterIndex(parameterIndex);
        if (x == null) {
            params.put(parameterIndex, "NULL");
        } else {
            params.put(parameterIndex, x.toPlainString());
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkParameterIndex(parameterIndex);
        if (x == null) {
            params.put(parameterIndex, "NULL");
        } else {
            // Reject dangerous characters that can bypass SQL escaping
            if (x.indexOf('\0') >= 0) {
                throw new SQLException("String parameter contains null byte (\\0) which is not allowed");
            }
            if (x.indexOf('\\') >= 0) {
                throw new SQLException("String parameter contains backslash (\\) which is not allowed "
                        + "in literal-interpolated queries. Use a plain Statement for backslash-containing values.");
            }
            // Escape single quotes by doubling them (standard SQL escaping)
            params.put(parameterIndex, "'" + x.replace("'", "''") + "'");
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkParameterIndex(parameterIndex);
        if (x == null) {
            params.put(parameterIndex, "NULL");
        } else {
            // Encode as hex literal
            StringBuilder sb = new StringBuilder("X'");
            for (byte b : x) {
                sb.append(String.format("%02X", b));
            }
            sb.append("'");
            params.put(parameterIndex, sb.toString());
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        checkParameterIndex(parameterIndex);
        if (x == null) {
            params.put(parameterIndex, "NULL");
        } else {
            params.put(parameterIndex, "'" + x.toString() + "'");
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkParameterIndex(parameterIndex);
        if (x == null) {
            params.put(parameterIndex, "NULL");
        } else {
            params.put(parameterIndex, "'" + x.toString() + "'");
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkParameterIndex(parameterIndex);
        if (x == null) {
            params.put(parameterIndex, "NULL");
        } else {
            params.put(parameterIndex, "'" + x.toString() + "'");
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkParameterIndex(parameterIndex);
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
        } else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        } else if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
        } else if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
        } else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        } else if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
        } else if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal) x);
        } else if (x instanceof Boolean) {
            setBoolean(parameterIndex, (Boolean) x);
        } else if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);
        } else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        } else if (x instanceof byte[]) {
            setBytes(parameterIndex, (byte[]) x);
        } else {
            // Default: treat as string
            setString(parameterIndex, x.toString());
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    // -------------------------------------------------------------------------
    // Parameter management
    // -------------------------------------------------------------------------

    @Override
    public void clearParameters() throws SQLException {
        params.clear();
        LOG.debug("clearParameters called");
    }

    // -------------------------------------------------------------------------
    // Batch support
    // -------------------------------------------------------------------------

    @Override
    public void addBatch() throws SQLException {
        batchSqls.add(resolveParameters());
        LOG.debug("addBatch: {} statements queued", batchSqls.size());
    }

    @Override
    public void clearBatch() throws SQLException {
        batchSqls.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        int[] results = new int[batchSqls.size()];
        for (int i = 0; i < batchSqls.size(); i++) {
            boolean hasResultSet = execute(batchSqls.get(i));
            results[i] = hasResultSet ? 0 : (updateCount >= 0 ? updateCount : 0);
        }
        batchSqls.clear();
        return results;
    }

    // -------------------------------------------------------------------------
    // Metadata (not fully implemented)
    // -------------------------------------------------------------------------

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        // Cannot determine result set metadata without executing the query
        return null;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("getParameterMetaData not supported");
    }

    // -------------------------------------------------------------------------
    // Unsupported setXxx methods (streams, refs, etc.)
    // -------------------------------------------------------------------------

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream not supported");
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setUnicodeStream not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream not supported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRef not supported");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob not supported");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob not supported");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setArray not supported");
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setURL not supported");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRowId not supported");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNCharacterStream not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNCharacterStream not supported");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob not supported");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSQLXML not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream not supported");
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Replaces {@code ?} placeholders in the SQL template with the stored parameter literals,
     * in left-to-right order (1-based indexing).
     *
     * @return the fully-resolved SQL string ready for execution
     * @throws SQLException if a required parameter has not been set
     */
    private String resolveParameters() throws SQLException {
        StringBuilder result = new StringBuilder();
        int paramIndex = 1;
        int pos = 0;

        while (pos < sqlTemplate.length()) {
            char c = sqlTemplate.charAt(pos);

            // Handle single-quoted string literals - do not substitute ? inside them
            if (c == '\'') {
                result.append(c);
                pos++;
                while (pos < sqlTemplate.length()) {
                    char inner = sqlTemplate.charAt(pos);
                    result.append(inner);
                    pos++;
                    if (inner == '\'') {
                        // Check for escaped quote ('')
                        if (pos < sqlTemplate.length() && sqlTemplate.charAt(pos) == '\'') {
                            result.append('\'');
                            pos++;
                        } else {
                            break;
                        }
                    }
                }
                continue;
            }

            // Handle parameter placeholder
            if (c == '?') {
                String paramValue = params.get(paramIndex);
                if (paramValue == null) {
                    throw new SQLException(
                            "Parameter " + paramIndex + " has not been set (SQL: " + sqlTemplate + ")");
                }
                result.append(paramValue);
                paramIndex++;
                pos++;
                continue;
            }

            result.append(c);
            pos++;
        }

        String resolvedSql = result.toString();
        LOG.debug("Resolved SQL: {}", resolvedSql);
        return resolvedSql;
    }

    private void checkParameterIndex(int parameterIndex) throws SQLException {
        if (parameterIndex < 1) {
            throw new SQLException("Parameter index must be >= 1, got " + parameterIndex);
        }
    }
}
