package com.keboola.jdbc.meta;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Maps Snowflake/Keboola data types to JDBC java.sql.Types constants.
 * Used by DatabaseMetaData and ResultSetMetaData to report column types.
 */
public final class TypeMapper {

    private static final Map<String, Integer> TYPE_MAP;
    private static final Map<String, String> JDBC_TYPE_NAME_MAP;
    private static final Map<String, Integer> DISPLAY_SIZE_MAP;

    static {
        Map<String, Integer> types = new HashMap<>();
        // String types
        types.put("VARCHAR", Types.VARCHAR);
        types.put("CHAR", Types.CHAR);
        types.put("STRING", Types.VARCHAR);
        types.put("TEXT", Types.VARCHAR);
        types.put("BINARY", Types.BINARY);
        types.put("VARBINARY", Types.VARBINARY);

        // Numeric types
        types.put("NUMBER", Types.DECIMAL);
        types.put("DECIMAL", Types.DECIMAL);
        types.put("NUMERIC", Types.NUMERIC);
        types.put("INT", Types.INTEGER);
        types.put("INTEGER", Types.INTEGER);
        types.put("BIGINT", Types.BIGINT);
        types.put("SMALLINT", Types.SMALLINT);
        types.put("TINYINT", Types.TINYINT);
        types.put("BYTEINT", Types.TINYINT);
        types.put("FLOAT", Types.FLOAT);
        types.put("FLOAT4", Types.FLOAT);
        types.put("FLOAT8", Types.DOUBLE);
        types.put("DOUBLE", Types.DOUBLE);
        types.put("DOUBLE PRECISION", Types.DOUBLE);
        types.put("REAL", Types.REAL);

        // Boolean
        types.put("BOOLEAN", Types.BOOLEAN);

        // Date/Time types
        types.put("DATE", Types.DATE);
        types.put("DATETIME", Types.TIMESTAMP);
        types.put("TIME", Types.TIME);
        types.put("TIMESTAMP", Types.TIMESTAMP);
        types.put("TIMESTAMP_LTZ", Types.TIMESTAMP);
        types.put("TIMESTAMP_NTZ", Types.TIMESTAMP);
        types.put("TIMESTAMP_TZ", Types.TIMESTAMP_WITH_TIMEZONE);

        // Semi-structured types (Snowflake-specific, map to VARCHAR for JDBC)
        types.put("VARIANT", Types.VARCHAR);
        types.put("OBJECT", Types.VARCHAR);
        types.put("ARRAY", Types.VARCHAR);

        TYPE_MAP = Collections.unmodifiableMap(types);

        // Display sizes for common types
        Map<String, Integer> sizes = new HashMap<>();
        sizes.put("VARCHAR", 16777216);
        sizes.put("CHAR", 1);
        sizes.put("STRING", 16777216);
        sizes.put("TEXT", 16777216);
        sizes.put("NUMBER", 38);
        sizes.put("DECIMAL", 38);
        sizes.put("NUMERIC", 38);
        sizes.put("INT", 11);
        sizes.put("INTEGER", 11);
        sizes.put("BIGINT", 20);
        sizes.put("SMALLINT", 6);
        sizes.put("TINYINT", 4);
        sizes.put("FLOAT", 24);
        sizes.put("DOUBLE", 53);
        sizes.put("BOOLEAN", 5);
        sizes.put("DATE", 10);
        sizes.put("TIME", 8);
        sizes.put("TIMESTAMP", 26);
        sizes.put("TIMESTAMP_LTZ", 26);
        sizes.put("TIMESTAMP_NTZ", 26);
        sizes.put("TIMESTAMP_TZ", 32);
        sizes.put("VARIANT", 16777216);
        sizes.put("OBJECT", 16777216);
        sizes.put("ARRAY", 16777216);
        DISPLAY_SIZE_MAP = Collections.unmodifiableMap(sizes);

        Map<String, String> names = new HashMap<>();
        names.put("STRING", "VARCHAR");
        names.put("TEXT", "VARCHAR");
        names.put("INT", "INTEGER");
        names.put("FLOAT4", "FLOAT");
        names.put("FLOAT8", "DOUBLE");
        names.put("DOUBLE PRECISION", "DOUBLE");
        names.put("BYTEINT", "TINYINT");
        names.put("DATETIME", "TIMESTAMP");
        JDBC_TYPE_NAME_MAP = Collections.unmodifiableMap(names);
    }

    private TypeMapper() {
        // Utility class
    }

    /**
     * Maps a Snowflake type name to a java.sql.Types constant.
     */
    public static int toJdbcType(String snowflakeType) {
        if (snowflakeType == null) {
            return Types.VARCHAR;
        }
        String upper = snowflakeType.toUpperCase().trim();
        // Handle parameterized types like VARCHAR(100) or NUMBER(38,0)
        int parenIdx = upper.indexOf('(');
        if (parenIdx > 0) {
            upper = upper.substring(0, parenIdx).trim();
        }
        return TYPE_MAP.getOrDefault(upper, Types.VARCHAR);
    }

    /**
     * Returns the canonical JDBC type name for a Snowflake type.
     */
    public static String toJdbcTypeName(String snowflakeType) {
        if (snowflakeType == null) {
            return "VARCHAR";
        }
        String upper = snowflakeType.toUpperCase().trim();
        int parenIdx = upper.indexOf('(');
        if (parenIdx > 0) {
            upper = upper.substring(0, parenIdx).trim();
        }
        return JDBC_TYPE_NAME_MAP.getOrDefault(upper, upper);
    }

    /**
     * Returns the display size for a given type.
     */
    public static int getDisplaySize(String snowflakeType, Integer length) {
        if (length != null && length > 0) {
            return length;
        }
        if (snowflakeType == null) {
            return 256;
        }
        String upper = snowflakeType.toUpperCase().trim();
        int parenIdx = upper.indexOf('(');
        if (parenIdx > 0) {
            upper = upper.substring(0, parenIdx).trim();
        }
        return DISPLAY_SIZE_MAP.getOrDefault(upper, 256);
    }

    /**
     * Returns the precision for a numeric type.
     */
    public static int getPrecision(String snowflakeType, Integer length) {
        if (length != null && length > 0) {
            return length;
        }
        int jdbcType = toJdbcType(snowflakeType);
        switch (jdbcType) {
            case Types.DECIMAL:
            case Types.NUMERIC:
                return 38;
            case Types.INTEGER:
                return 10;
            case Types.BIGINT:
                return 19;
            case Types.SMALLINT:
                return 5;
            case Types.TINYINT:
                return 3;
            case Types.FLOAT:
            case Types.REAL:
                return 24;
            case Types.DOUBLE:
                return 53;
            default:
                return getDisplaySize(snowflakeType, length);
        }
    }

    /**
     * Returns the scale for a numeric type.
     */
    public static int getScale(String snowflakeType) {
        int jdbcType = toJdbcType(snowflakeType);
        switch (jdbcType) {
            case Types.DECIMAL:
            case Types.NUMERIC:
                return 0; // Default scale, actual comes from column metadata
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
                return 0;
            default:
                return 0;
        }
    }

    /**
     * Alias for {@link #toJdbcTypeName(String)} - returns the SQL type name.
     * Provided for compatibility with ResultSetMetaData.getColumnTypeName().
     */
    public static String toSqlTypeName(String snowflakeType) {
        return toJdbcTypeName(snowflakeType);
    }

    /**
     * Returns the fully-qualified Java class name for the given Keboola/Snowflake type.
     * Equivalent to {@link #getClassName(String)} - provided for ResultSetMetaData compatibility.
     */
    public static String toJavaClassName(String snowflakeType) {
        return getClassName(snowflakeType);
    }

    /**
     * Returns true if the given JDBC type is a signed numeric type.
     * Used by ResultSetMetaData.isSigned().
     *
     * @param jdbcType a constant from {@link java.sql.Types}
     */
    public static boolean isSigned(int jdbcType) {
        switch (jdbcType) {
            case Types.DECIMAL:
            case Types.NUMERIC:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns the Java class name for a JDBC type.
     */
    public static String getClassName(String snowflakeType) {
        int jdbcType = toJdbcType(snowflakeType);
        switch (jdbcType) {
            case Types.VARCHAR:
            case Types.CHAR:
                return "java.lang.String";
            case Types.DECIMAL:
            case Types.NUMERIC:
                return "java.math.BigDecimal";
            case Types.INTEGER:
                return "java.lang.Integer";
            case Types.BIGINT:
                return "java.lang.Long";
            case Types.SMALLINT:
                return "java.lang.Short";
            case Types.TINYINT:
                return "java.lang.Byte";
            case Types.FLOAT:
            case Types.REAL:
                return "java.lang.Float";
            case Types.DOUBLE:
                return "java.lang.Double";
            case Types.BOOLEAN:
                return "java.lang.Boolean";
            case Types.DATE:
                return "java.sql.Date";
            case Types.TIME:
                return "java.sql.Time";
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return "java.sql.Timestamp";
            case Types.BINARY:
            case Types.VARBINARY:
                return "byte[]";
            default:
                return "java.lang.String";
        }
    }
}
