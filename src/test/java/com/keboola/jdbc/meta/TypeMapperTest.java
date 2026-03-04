package com.keboola.jdbc.meta;

import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TypeMapper - verifies Snowflake/Keboola type mapping to JDBC types.
 */
class TypeMapperTest {

    // -------------------------------------------------------------------------
    // toJdbcType() tests
    // -------------------------------------------------------------------------

    @Test
    void toJdbcType_varchar_returnsVarchar() {
        assertEquals(Types.VARCHAR, TypeMapper.toJdbcType("VARCHAR"));
    }

    @Test
    void toJdbcType_number_returnsDecimal() {
        assertEquals(Types.DECIMAL, TypeMapper.toJdbcType("NUMBER"));
    }

    @Test
    void toJdbcType_integer_returnsInteger() {
        assertEquals(Types.INTEGER, TypeMapper.toJdbcType("INTEGER"));
    }

    @Test
    void toJdbcType_int_returnsInteger() {
        assertEquals(Types.INTEGER, TypeMapper.toJdbcType("INT"));
    }

    @Test
    void toJdbcType_boolean_returnsBoolean() {
        assertEquals(Types.BOOLEAN, TypeMapper.toJdbcType("BOOLEAN"));
    }

    @Test
    void toJdbcType_date_returnsDate() {
        assertEquals(Types.DATE, TypeMapper.toJdbcType("DATE"));
    }

    @Test
    void toJdbcType_timestamp_returnsTimestamp() {
        assertEquals(Types.TIMESTAMP, TypeMapper.toJdbcType("TIMESTAMP"));
    }

    @Test
    void toJdbcType_timestampTz_returnsTimestampWithTimezone() {
        assertEquals(Types.TIMESTAMP_WITH_TIMEZONE, TypeMapper.toJdbcType("TIMESTAMP_TZ"));
    }

    @Test
    void toJdbcType_variant_returnsVarchar() {
        // Snowflake semi-structured types map to VARCHAR for JDBC compatibility
        assertEquals(Types.VARCHAR, TypeMapper.toJdbcType("VARIANT"));
    }

    @Test
    void toJdbcType_float_returnsFloat() {
        assertEquals(Types.FLOAT, TypeMapper.toJdbcType("FLOAT"));
    }

    @Test
    void toJdbcType_double_returnsDouble() {
        assertEquals(Types.DOUBLE, TypeMapper.toJdbcType("DOUBLE"));
    }

    @Test
    void toJdbcType_caseInsensitive_mapsCorrectly() {
        assertEquals(Types.VARCHAR, TypeMapper.toJdbcType("varchar"));
        assertEquals(Types.INTEGER, TypeMapper.toJdbcType("integer"));
        assertEquals(Types.BOOLEAN, TypeMapper.toJdbcType("boolean"));
    }

    @Test
    void toJdbcType_null_defaultsToVarchar() {
        assertEquals(Types.VARCHAR, TypeMapper.toJdbcType(null));
    }

    @Test
    void toJdbcType_unknownType_defaultsToVarchar() {
        assertEquals(Types.VARCHAR, TypeMapper.toJdbcType("UNKNOWN_TYPE_XYZ"));
    }

    @Test
    void toJdbcType_varcharWithLength_stripsParamsAndMapsCorrectly() {
        assertEquals(Types.VARCHAR, TypeMapper.toJdbcType("VARCHAR(100)"));
    }

    @Test
    void toJdbcType_numberWithPrecisionScale_stripsParamsAndMapsCorrectly() {
        assertEquals(Types.DECIMAL, TypeMapper.toJdbcType("NUMBER(38,0)"));
    }

    @Test
    void toJdbcType_charWithLength_stripsParamsAndMapsCorrectly() {
        assertEquals(Types.CHAR, TypeMapper.toJdbcType("CHAR(10)"));
    }

    @Test
    void toJdbcType_bigint_returnsBigint() {
        assertEquals(Types.BIGINT, TypeMapper.toJdbcType("BIGINT"));
    }

    @Test
    void toJdbcType_timestampLtz_returnsTimestamp() {
        assertEquals(Types.TIMESTAMP, TypeMapper.toJdbcType("TIMESTAMP_LTZ"));
    }

    @Test
    void toJdbcType_timestampNtz_returnsTimestamp() {
        assertEquals(Types.TIMESTAMP, TypeMapper.toJdbcType("TIMESTAMP_NTZ"));
    }

    // -------------------------------------------------------------------------
    // toJdbcTypeName() tests
    // -------------------------------------------------------------------------

    @Test
    void toJdbcTypeName_varchar_returnsVarchar() {
        assertEquals("VARCHAR", TypeMapper.toJdbcTypeName("VARCHAR"));
    }

    @Test
    void toJdbcTypeName_string_returnsMappedToVarchar() {
        // STRING is a Snowflake alias; canonical JDBC name is VARCHAR
        assertEquals("VARCHAR", TypeMapper.toJdbcTypeName("STRING"));
    }

    @Test
    void toJdbcTypeName_text_returnsMappedToVarchar() {
        assertEquals("VARCHAR", TypeMapper.toJdbcTypeName("TEXT"));
    }

    @Test
    void toJdbcTypeName_int_returnsMappedToInteger() {
        assertEquals("INTEGER", TypeMapper.toJdbcTypeName("INT"));
    }

    @Test
    void toJdbcTypeName_float8_returnsMappedToDouble() {
        assertEquals("DOUBLE", TypeMapper.toJdbcTypeName("FLOAT8"));
    }

    @Test
    void toJdbcTypeName_null_returnsVarchar() {
        assertEquals("VARCHAR", TypeMapper.toJdbcTypeName(null));
    }

    @Test
    void toJdbcTypeName_number_returnsNumber() {
        // NUMBER has no alias mapping, returns itself
        assertEquals("NUMBER", TypeMapper.toJdbcTypeName("NUMBER"));
    }

    @Test
    void toJdbcTypeName_varcharWithLength_stripsParams() {
        assertEquals("VARCHAR", TypeMapper.toJdbcTypeName("VARCHAR(255)"));
    }

    // -------------------------------------------------------------------------
    // getDisplaySize() tests
    // -------------------------------------------------------------------------

    @Test
    void getDisplaySize_withExplicitLength_returnsProvidedLength() {
        assertEquals(200, TypeMapper.getDisplaySize("VARCHAR", 200));
    }

    @Test
    void getDisplaySize_withNullLength_usesTypeDefault() {
        assertEquals(16777216, TypeMapper.getDisplaySize("VARCHAR", null));
    }

    @Test
    void getDisplaySize_withZeroLength_usesTypeDefault() {
        assertEquals(16777216, TypeMapper.getDisplaySize("VARCHAR", 0));
    }

    @Test
    void getDisplaySize_integerType_returnsExpectedSize() {
        assertEquals(11, TypeMapper.getDisplaySize("INTEGER", null));
    }

    @Test
    void getDisplaySize_dateType_returnsTenChars() {
        assertEquals(10, TypeMapper.getDisplaySize("DATE", null));
    }

    @Test
    void getDisplaySize_timestampTzType_returns32() {
        assertEquals(32, TypeMapper.getDisplaySize("TIMESTAMP_TZ", null));
    }

    @Test
    void getDisplaySize_nullType_returnsDefaultFallback() {
        // Null type falls back to 256
        assertEquals(256, TypeMapper.getDisplaySize(null, null));
    }

    @Test
    void getDisplaySize_unknownType_returnsFallback() {
        assertEquals(256, TypeMapper.getDisplaySize("UNKNOWN_TYPE", null));
    }

    // -------------------------------------------------------------------------
    // getPrecision() tests
    // -------------------------------------------------------------------------

    @Test
    void getPrecision_numberType_returns38() {
        assertEquals(38, TypeMapper.getPrecision("NUMBER", null));
    }

    @Test
    void getPrecision_integerType_returns10() {
        assertEquals(10, TypeMapper.getPrecision("INTEGER", null));
    }

    @Test
    void getPrecision_bigintType_returns19() {
        assertEquals(19, TypeMapper.getPrecision("BIGINT", null));
    }

    @Test
    void getPrecision_floatType_returns24() {
        assertEquals(24, TypeMapper.getPrecision("FLOAT", null));
    }

    @Test
    void getPrecision_doubleType_returns53() {
        assertEquals(53, TypeMapper.getPrecision("DOUBLE", null));
    }

    @Test
    void getPrecision_withExplicitLength_returnsProvidedLength() {
        assertEquals(50, TypeMapper.getPrecision("VARCHAR", 50));
    }

    @Test
    void getPrecision_varcharWithNoLength_usesDisplaySize() {
        // VARCHAR has no numeric precision; falls back to display size
        assertEquals(16777216, TypeMapper.getPrecision("VARCHAR", null));
    }

    // -------------------------------------------------------------------------
    // getClassName() tests
    // -------------------------------------------------------------------------

    @Test
    void getClassName_varchar_returnsStringClass() {
        assertEquals("java.lang.String", TypeMapper.getClassName("VARCHAR"));
    }

    @Test
    void getClassName_number_returnsBigDecimalClass() {
        assertEquals("java.math.BigDecimal", TypeMapper.getClassName("NUMBER"));
    }

    @Test
    void getClassName_integer_returnsIntegerClass() {
        assertEquals("java.lang.Integer", TypeMapper.getClassName("INTEGER"));
    }

    @Test
    void getClassName_bigint_returnsLongClass() {
        assertEquals("java.lang.Long", TypeMapper.getClassName("BIGINT"));
    }

    @Test
    void getClassName_boolean_returnsBooleanClass() {
        assertEquals("java.lang.Boolean", TypeMapper.getClassName("BOOLEAN"));
    }

    @Test
    void getClassName_date_returnsSqlDateClass() {
        assertEquals("java.sql.Date", TypeMapper.getClassName("DATE"));
    }

    @Test
    void getClassName_timestamp_returnsSqlTimestampClass() {
        assertEquals("java.sql.Timestamp", TypeMapper.getClassName("TIMESTAMP"));
    }

    @Test
    void getClassName_timestampTz_returnsSqlTimestampClass() {
        assertEquals("java.sql.Timestamp", TypeMapper.getClassName("TIMESTAMP_TZ"));
    }

    @Test
    void getClassName_double_returnsDoubleClass() {
        assertEquals("java.lang.Double", TypeMapper.getClassName("DOUBLE"));
    }

    @Test
    void getClassName_float_returnsFloatClass() {
        assertEquals("java.lang.Float", TypeMapper.getClassName("FLOAT"));
    }

    @Test
    void getClassName_variant_returnsStringClass() {
        // VARIANT maps to VARCHAR -> String
        assertEquals("java.lang.String", TypeMapper.getClassName("VARIANT"));
    }

    @Test
    void getClassName_unknownType_returnsStringClass() {
        assertEquals("java.lang.String", TypeMapper.getClassName("UNKNOWN_TYPE"));
    }
}
