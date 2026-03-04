package com.keboola.jdbc;

import com.keboola.jdbc.http.model.ResultColumn;
import com.keboola.jdbc.meta.TypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

/**
 * ResultSetMetaData implementation backed by a list of {@link ResultColumn} descriptors
 * returned by the Keboola Query Service.
 *
 * <p>Column indices are 1-based as per the JDBC specification.
 */
public class KeboolaResultSetMetaData implements ResultSetMetaData {

    private static final Logger LOG = LoggerFactory.getLogger(KeboolaResultSetMetaData.class);

    private final List<ResultColumn> columns;

    /**
     * Creates metadata for the given column list.
     *
     * @param columns ordered list of result columns (must not be null)
     */
    public KeboolaResultSetMetaData(List<ResultColumn> columns) {
        this.columns = columns;
    }

    // -------------------------------------------------------------------------
    // Column count
    // -------------------------------------------------------------------------

    @Override
    public int getColumnCount() throws SQLException {
        return columns.size();
    }

    // -------------------------------------------------------------------------
    // Column identification
    // -------------------------------------------------------------------------

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumn(column).getName();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        // Use name as label - the Query Service does not distinguish between the two
        return getColumn(column).getName();
    }

    // -------------------------------------------------------------------------
    // Type information (delegated to TypeMapper)
    // -------------------------------------------------------------------------

    @Override
    public int getColumnType(int column) throws SQLException {
        return TypeMapper.toJdbcType(getColumn(column).getType());
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return TypeMapper.toSqlTypeName(getColumn(column).getType());
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return TypeMapper.toJavaClassName(getColumn(column).getType());
    }

    // -------------------------------------------------------------------------
    // Nullability
    // -------------------------------------------------------------------------

    @Override
    public int isNullable(int column) throws SQLException {
        ResultColumn col = getColumn(column);
        if (col.isNullable() != null) {
            return col.isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
        }
        return ResultSetMetaData.columnNullableUnknown;
    }

    // -------------------------------------------------------------------------
    // Size / precision / scale
    // -------------------------------------------------------------------------

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        ResultColumn col = getColumn(column);
        Integer length = col.getLength();
        return (length != null && length > 0) ? length : 255;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        ResultColumn col = getColumn(column);
        Integer precision = col.getPrecision();
        if (precision != null && precision > 0) {
            return precision;
        }
        Integer length = col.getLength();
        return (length != null && length > 0) ? length : 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        ResultColumn col = getColumn(column);
        Integer scale = col.getScale();
        return (scale != null) ? scale : 0;
    }

    // -------------------------------------------------------------------------
    // Table / schema / catalog (not available from query results)
    // -------------------------------------------------------------------------

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    // -------------------------------------------------------------------------
    // Column characteristics
    // -------------------------------------------------------------------------

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        checkIndex(column);
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        checkIndex(column);
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        checkIndex(column);
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        checkIndex(column);
        return false;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        checkIndex(column);
        int jdbcType = TypeMapper.toJdbcType(getColumn(column).getType());
        return TypeMapper.isSigned(jdbcType);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        checkIndex(column);
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        checkIndex(column);
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        checkIndex(column);
        return false;
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
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Returns the column descriptor at the given 1-based index.
     *
     * @param column 1-based column index
     * @return the ResultColumn descriptor
     * @throws SQLException if the index is out of range
     */
    private ResultColumn getColumn(int column) throws SQLException {
        checkIndex(column);
        return columns.get(column - 1);
    }

    private void checkIndex(int column) throws SQLException {
        if (column < 1 || column > columns.size()) {
            throw new SQLException("Column index out of range: " + column
                    + " (expected 1.." + columns.size() + ")");
        }
    }
}
