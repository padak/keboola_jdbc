package com.keboola.jdbc;

import com.keboola.jdbc.config.DriverConfig;
import com.keboola.jdbc.http.QueryServiceClient;
import com.keboola.jdbc.http.model.QueryResult;
import com.keboola.jdbc.http.model.ResultColumn;
import com.keboola.jdbc.util.EpochConverter;
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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * Lazy-paging ResultSet for query results fetched from the Keboola Query Service.
 *
 * <p>Rows are loaded in pages of {@value #PAGE_SIZE} rows. When the current page is exhausted,
 * the next page is fetched automatically from the server using the accumulated row offset.
 *
 * <p>Column indices are 1-based as per the JDBC specification.
 */
public class KeboolaResultSet implements ResultSet {

    private static final Logger LOG = LoggerFactory.getLogger(KeboolaResultSet.class);

    /** Number of rows fetched per page from the Query Service. API minimum is 100. */
    static final int PAGE_SIZE = DriverConfig.DEFAULT_PAGE_SIZE;

    private final QueryServiceClient client;
    private final String queryJobId;
    private final String statementId;
    private final List<ResultColumn> columns;

    /** Rows in the current page; each row is a list of String values (may contain nulls). */
    private List<List<String>> currentPage;

    /** 0-based index within {@link #currentPage} for the current row. */
    private int currentRowIndex = -1;

    /** Total number of rows fetched across all pages so far. */
    private int totalRowsFetched = 0;

    /** Whether the last fetched value was null. */
    private boolean lastWasNull = false;

    /**
     * Whether there are more pages to fetch from the server.
     * Determined by checking if the last fetched page was full (= PAGE_SIZE rows),
     * because the Query Service API does not reliably return a hasMorePages flag.
     */
    private boolean hasMorePages = true;

    /** The first page of results, retained for backendContext access. */
    private final QueryResult firstPageResult;

    private boolean closed = false;

    /**
     * Creates a KeboolaResultSet from the first page of query results.
     *
     * @param client      the Query Service HTTP client used for subsequent page fetches
     * @param queryJobId  the job ID used to cancel the job if needed
     * @param statementId the statement ID used for paging requests
     * @param firstPage   the first page of results (columns + data)
     */
    public KeboolaResultSet(QueryServiceClient client,
                            String queryJobId,
                            String statementId,
                            QueryResult firstPage) {
        this.client = client;
        this.queryJobId = queryJobId;
        this.statementId = statementId;
        this.columns = firstPage.getColumns();
        this.currentPage = firstPage.getData();
        this.hasMorePages = currentPage.size() >= PAGE_SIZE;
        this.firstPageResult = firstPage;
        LOG.debug("KeboolaResultSet created: statementId={}, columns={}, firstPageRows={}",
                statementId, columns.size(), currentPage.size());
    }

    /** Returns the first page result, used to read backendContext. */
    QueryResult getFirstPageResult() {
        return firstPageResult;
    }

    // -------------------------------------------------------------------------
    // Cursor movement
    // -------------------------------------------------------------------------

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        currentRowIndex++;

        if (currentRowIndex < currentPage.size()) {
            return true;
        }

        // Current page exhausted - try to fetch next page
        if (!hasMorePages) {
            return false;
        }

        totalRowsFetched += currentPage.size();
        fetchNextPage();

        if (currentPage.isEmpty()) {
            return false;
        }

        currentRowIndex = 0;
        return true;
    }

    /**
     * Fetches the next page of results from the Query Service using the current row offset.
     *
     * @throws SQLException if the page fetch fails
     */
    private void fetchNextPage() throws SQLException {
        LOG.debug("Fetching next page for statementId={}, offset={}", statementId, totalRowsFetched);
        try {
            QueryResult nextResult = client.fetchResults(queryJobId, statementId, totalRowsFetched, PAGE_SIZE);
            currentPage = nextResult.getData();
            hasMorePages = currentPage.size() >= PAGE_SIZE;
            currentRowIndex = -1;
            LOG.debug("Fetched {} rows, hasMorePages={}", currentPage.size(), hasMorePages);
        } catch (Exception e) {
            throw new SQLException("Failed to fetch next page of results: " + e.getMessage(), e);
        }
    }

    // -------------------------------------------------------------------------
    // Column index resolution (1-based)
    // -------------------------------------------------------------------------

    /**
     * Resolves the 1-based column index from a column label (case-insensitive).
     *
     * @param columnLabel the column name/label
     * @return the 1-based column index
     * @throws SQLException if the column label is not found
     */
    private int findColumnIndex(String columnLabel) throws SQLException {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(columnLabel)) {
                return i + 1;
            }
        }
        throw new SQLException("Column not found: " + columnLabel);
    }

    /**
     * Returns the raw String value at the given 1-based column index in the current row.
     *
     * @param columnIndex 1-based column index
     * @return the String value, or null if the cell contains null
     * @throws SQLException if not positioned on a valid row or index is out of range
     */
    private String getRawValue(int columnIndex) throws SQLException {
        checkClosed();
        checkRow();
        if (columnIndex < 1 || columnIndex > columns.size()) {
            throw new SQLException("Column index out of range: " + columnIndex
                    + " (expected 1.." + columns.size() + ")");
        }
        List<String> row = currentPage.get(currentRowIndex);
        String value = (columnIndex - 1 < row.size()) ? row.get(columnIndex - 1) : null;
        lastWasNull = (value == null);
        return value;
    }

    /**
     * Returns the Snowflake type name for the given 1-based column index.
     */
    private String getSnowflakeType(int columnIndex) {
        if (columnIndex >= 1 && columnIndex <= columns.size()) {
            return columns.get(columnIndex - 1).getType();
        }
        return null;
    }

    // -------------------------------------------------------------------------
    // getString and typed getters
    // -------------------------------------------------------------------------

    @Override
    public String getString(int columnIndex) throws SQLException {
        String raw = getRawValue(columnIndex);
        if (raw == null) return null;
        return EpochConverter.formatValue(raw, getSnowflakeType(columnIndex));
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumnIndex(columnLabel));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        String val = getRawValue(columnIndex);
        if (val == null) {
            return 0;
        }
        try {
            return Integer.parseInt(val.trim());
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + val + "' to int", e);
        }
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumnIndex(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        String val = getRawValue(columnIndex);
        if (val == null) {
            return 0L;
        }
        try {
            return Long.parseLong(val.trim());
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + val + "' to long", e);
        }
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumnIndex(columnLabel));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        String val = getRawValue(columnIndex);
        if (val == null) {
            return 0.0;
        }
        try {
            return Double.parseDouble(val.trim());
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + val + "' to double", e);
        }
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumnIndex(columnLabel));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        String val = getRawValue(columnIndex);
        if (val == null) {
            return 0.0f;
        }
        try {
            return Float.parseFloat(val.trim());
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + val + "' to float", e);
        }
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumnIndex(columnLabel));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        String val = getRawValue(columnIndex);
        if (val == null) {
            return false;
        }
        return "true".equalsIgnoreCase(val.trim()) || "1".equals(val.trim());
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumnIndex(columnLabel));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        String val = getRawValue(columnIndex);
        if (val == null) {
            return 0;
        }
        try {
            return Short.parseShort(val.trim());
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + val + "' to short", e);
        }
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumnIndex(columnLabel));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        String val = getRawValue(columnIndex);
        if (val == null) {
            return 0;
        }
        try {
            return Byte.parseByte(val.trim());
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + val + "' to byte", e);
        }
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumnIndex(columnLabel));
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        String val = getRawValue(columnIndex);
        return val == null ? null : val.getBytes();
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumnIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        String val = getRawValue(columnIndex);
        if (val == null) {
            return null;
        }
        try {
            return new BigDecimal(val.trim());
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + val + "' to BigDecimal", e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumnIndex(columnLabel));
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal bd = getBigDecimal(columnIndex);
        return bd == null ? null : bd.setScale(scale);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumnIndex(columnLabel), scale);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        String val = getRawValue(columnIndex);
        if (val == null) {
            return null;
        }
        try {
            String sfType = getSnowflakeType(columnIndex);
            if (EpochConverter.isDateType(sfType)) {
                return EpochConverter.toDate(val);
            }
            return Date.valueOf(val.trim());
        } catch (Exception e) {
            throw new SQLException("Cannot convert '" + val + "' to Date", e);
        }
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumnIndex(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumnIndex(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        String val = getRawValue(columnIndex);
        if (val == null) {
            return null;
        }
        try {
            String sfType = getSnowflakeType(columnIndex);
            if (EpochConverter.isTimeType(sfType)) {
                return EpochConverter.toTime(val);
            }
            return Time.valueOf(val.trim());
        } catch (Exception e) {
            throw new SQLException("Cannot convert '" + val + "' to Time", e);
        }
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumnIndex(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumnIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        String val = getRawValue(columnIndex);
        if (val == null) {
            return null;
        }
        try {
            String sfType = getSnowflakeType(columnIndex);
            if (EpochConverter.isTimestampType(sfType)) {
                return EpochConverter.toTimestamp(val);
            }
            return Timestamp.valueOf(val.trim());
        } catch (Exception e) {
            throw new SQLException("Cannot convert '" + val + "' to Timestamp", e);
        }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumnIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumnIndex(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        String val = getRawValue(columnIndex);
        if (val == null) return null;
        String sfType = getSnowflakeType(columnIndex);
        try {
            if (EpochConverter.isDateType(sfType)) {
                return EpochConverter.toDate(val);
            }
            if (EpochConverter.isTimestampType(sfType)) {
                return EpochConverter.toTimestamp(val);
            }
            if (EpochConverter.isTimeType(sfType)) {
                return EpochConverter.toTime(val);
            }
        } catch (Exception e) {
            LOG.debug("Could not convert value '{}' for type '{}', returning as String", val, sfType);
        }
        return val;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumnIndex(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject with type map not supported");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject with type map not supported");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject with class not supported");
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject with class not supported");
    }

    // -------------------------------------------------------------------------
    // Null tracking
    // -------------------------------------------------------------------------

    @Override
    public boolean wasNull() throws SQLException {
        return lastWasNull;
    }

    // -------------------------------------------------------------------------
    // Metadata
    // -------------------------------------------------------------------------

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return new KeboolaResultSetMetaData(columns);
    }

    // -------------------------------------------------------------------------
    // ResultSet properties
    // -------------------------------------------------------------------------

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
            LOG.debug("KeboolaResultSet closed: statementId={}, totalRowsFetched={}", statementId, totalRowsFetched);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("Only FETCH_FORWARD is supported");
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return PAGE_SIZE;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // ignored - page size is fixed
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // no-op
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("getCursorName not supported");
    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return findColumnIndex(columnLabel);
    }

    // -------------------------------------------------------------------------
    // Cursor position checks (only FORWARD is supported)
    // -------------------------------------------------------------------------

    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("isBeforeFirst not supported for forward-only ResultSet");
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("isAfterLast not supported for forward-only ResultSet");
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("isFirst not supported for forward-only ResultSet");
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("isLast not supported for forward-only ResultSet");
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("beforeFirst not supported for forward-only ResultSet");
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("afterLast not supported for forward-only ResultSet");
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException("first not supported for forward-only ResultSet");
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException("last not supported for forward-only ResultSet");
    }

    @Override
    public int getRow() throws SQLException {
        return totalRowsFetched + currentRowIndex + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException("absolute not supported for forward-only ResultSet");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("relative not supported for forward-only ResultSet");
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("previous not supported for forward-only ResultSet");
    }

    // -------------------------------------------------------------------------
    // Unsupported stream / ref / blob / etc. getters
    // -------------------------------------------------------------------------

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getAsciiStream not supported");
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getAsciiStream not supported");
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getUnicodeStream not supported");
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getUnicodeStream not supported");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBinaryStream not supported");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBinaryStream not supported");
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCharacterStream not supported");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCharacterStream not supported");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef not supported");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef not supported");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBlob not supported");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBlob not supported");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClob not supported");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClob not supported");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray not supported");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray not supported");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getURL not supported");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getURL not supported");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowId not supported");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowId not supported");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNClob not supported");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNClob not supported");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML not supported");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML not supported");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNCharacterStream not supported");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNCharacterStream not supported");
    }

    // -------------------------------------------------------------------------
    // Unsupported update methods (read-only ResultSet)
    // -------------------------------------------------------------------------

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNull not supported");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBoolean not supported");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateByte not supported");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateShort not supported");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateInt not supported");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateLong not supported");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateFloat not supported");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDouble not supported");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBigDecimal not supported");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateString not supported");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBytes not supported");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDate not supported");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTime not supported");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTimestamp not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject not supported");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject not supported");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNull not supported");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBoolean not supported");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateByte not supported");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateShort not supported");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateInt not supported");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateLong not supported");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateFloat not supported");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDouble not supported");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBigDecimal not supported");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateString not supported");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBytes not supported");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateDate not supported");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTime not supported");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateTimestamp not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject not supported");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateObject not supported");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("insertRow not supported");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRow not supported");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("deleteRow not supported");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("refreshRow not supported");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("cancelRowUpdates not supported");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("moveToInsertRow not supported");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("moveToCurrentRow not supported");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRef not supported");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRef not supported");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob not supported");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob not supported");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob not supported");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob not supported");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateArray not supported");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateArray not supported");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRowId not supported");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateRowId not supported");
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNString not supported");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNString not supported");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob not supported");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob not supported");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateSQLXML not supported");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateSQLXML not supported");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream not supported");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob not supported");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob not supported");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob not supported");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob not supported");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob not supported");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob not supported");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream not supported");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream not supported");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateAsciiStream not supported");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBinaryStream not supported");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateCharacterStream not supported");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob not supported");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateBlob not supported");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob not supported");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateClob not supported");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob not supported");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("updateNClob not supported");
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

    @Override
    public int getHoldability() throws SQLException {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }

    private void checkRow() throws SQLException {
        if (currentRowIndex < 0 || currentRowIndex >= currentPage.size()) {
            throw new SQLException("ResultSet not positioned on a valid row");
        }
    }
}
