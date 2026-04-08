package com.keboola.jdbc.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for EpochConverter — verifies conversion of Snowflake epoch-based
 * values to Java SQL date/time types.
 *
 * <p>Test values are cross-referenced with the Keboola UI test suite
 * (helpers.test.ts) for consistency.
 */
class EpochConverterTest {

    // -------------------------------------------------------------------------
    // Type detection
    // -------------------------------------------------------------------------

    @Test
    void isDateType_date_returnsTrue() {
        assertTrue(EpochConverter.isDateType("date"));
        assertTrue(EpochConverter.isDateType("DATE"));
        assertTrue(EpochConverter.isDateType("Date"));
    }

    @Test
    void isDateType_nonDate_returnsFalse() {
        assertFalse(EpochConverter.isDateType("timestamp_ltz"));
        assertFalse(EpochConverter.isDateType("varchar"));
        assertFalse(EpochConverter.isDateType(null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"timestamp_tz", "TIMESTAMP_LTZ", "timestamp_ntz", "DATETIME", "timestamp"})
    void isTimestampType_variousTimestamps_returnsTrue(String type) {
        assertTrue(EpochConverter.isTimestampType(type));
    }

    @Test
    void isTimestampType_nonTimestamp_returnsFalse() {
        assertFalse(EpochConverter.isTimestampType("date"));
        assertFalse(EpochConverter.isTimestampType("time"));
        assertFalse(EpochConverter.isTimestampType(null));
    }

    @Test
    void isTimeType_time_returnsTrue() {
        assertTrue(EpochConverter.isTimeType("time"));
        assertTrue(EpochConverter.isTimeType("TIME"));
    }

    @Test
    void isTimeType_nonTime_returnsFalse() {
        assertFalse(EpochConverter.isTimeType("date"));
        assertFalse(EpochConverter.isTimeType(null));
    }

    // -------------------------------------------------------------------------
    // DATE conversion
    // -------------------------------------------------------------------------

    @Test
    void toDate_epochDays_convertsCorrectly() {
        // 20551 days = 2026-04-08 (verified via Query Service)
        Date result = EpochConverter.toDate("20551");
        assertEquals(Date.valueOf("2026-04-08"), result);
    }

    @Test
    void toDate_zero_returnsEpoch() {
        // Keboola UI test: "0" → "1970-01-01"
        assertEquals(Date.valueOf("1970-01-01"), EpochConverter.toDate("0"));
    }

    @Test
    void toDate_negativeDays_returnsBeforeEpoch() {
        // Keboola UI test: "-1" → "1969-12-31"
        assertEquals(Date.valueOf("1969-12-31"), EpochConverter.toDate("-1"));
    }

    @Test
    void toDate_20398_matches_ui_test() {
        // Keboola UI test: 20398 → "2025-11-06"
        assertEquals(Date.valueOf("2025-11-06"), EpochConverter.toDate("20398"));
    }

    @Test
    void toDate_20171_matches_castDate() {
        // From our Query Service test: CAST_DATE '2025-03-24'::DATE = 20171
        assertEquals(Date.valueOf("2025-03-24"), EpochConverter.toDate("20171"));
    }

    @Test
    void toDate_null_returnsNull() {
        assertNull(EpochConverter.toDate(null));
    }

    @Test
    void toDate_invalidString_throwsException() {
        assertThrows(NumberFormatException.class, () -> EpochConverter.toDate("invalid"));
    }

    @Test
    void formatDate_convertsToIsoString() {
        assertEquals("2026-04-08", EpochConverter.formatDate("20551"));
        assertEquals("1970-01-01", EpochConverter.formatDate("0"));
        assertEquals("1969-12-31", EpochConverter.formatDate("-1"));
    }

    @Test
    void formatDate_invalidInput_returnsOriginal() {
        assertEquals("invalid", EpochConverter.formatDate("invalid"));
    }

    // -------------------------------------------------------------------------
    // TIMESTAMP conversion
    // -------------------------------------------------------------------------

    @Test
    void toTimestamp_epochSeconds_convertsCorrectly() {
        // 1775677092.984000000 = 2026-04-08T12:38:12.984Z (verified via Query Service)
        Timestamp result = EpochConverter.toTimestamp("1775677092.984000000");
        assertEquals(1775677092L * 1000, result.getTime() - (result.getTime() % 1000));
        assertEquals(984000000, result.getNanos());
    }

    @Test
    void toTimestamp_zero_returnsEpoch() {
        // Keboola UI test: "0" → "1970-01-01T00:00:00Z"
        Timestamp result = EpochConverter.toTimestamp("0");
        assertEquals(0L, result.getTime());
        assertEquals(0, result.getNanos());
    }

    @Test
    void toTimestamp_integerOnly_noFractionalPart() {
        // Keboola UI test: "1762498275"
        Timestamp result = EpochConverter.toTimestamp("1762498275");
        assertNotNull(result);
        assertEquals(0, result.getNanos());
    }

    @Test
    void toTimestamp_timestampTzWithOffset_stripsOffset() {
        // Snowflake TIMESTAMP_TZ format: "seconds.nanos offset_minutes"
        Timestamp result = EpochConverter.toTimestamp("1775677092.984000000 1560");
        assertEquals(984000000, result.getNanos());
    }

    @Test
    void toTimestamp_null_returnsNull() {
        assertNull(EpochConverter.toTimestamp(null));
    }

    @Test
    void formatTimestamp_withTimezone_appendsZ() {
        String result = EpochConverter.formatTimestamp("0", true);
        assertTrue(result.endsWith("Z"), "Expected Z suffix, got: " + result);
        assertTrue(result.contains("1970-01-01"), "Expected epoch date, got: " + result);
    }

    @Test
    void formatTimestamp_withoutTimezone_noZ() {
        String result = EpochConverter.formatTimestamp("0", false);
        assertFalse(result.endsWith("Z"), "Expected no Z suffix, got: " + result);
        assertTrue(result.contains("1970-01-01"), "Expected epoch date, got: " + result);
    }

    @Test
    void formatTimestamp_invalidInput_returnsOriginal() {
        assertEquals("invalid", EpochConverter.formatTimestamp("invalid", true));
    }

    // -------------------------------------------------------------------------
    // TIME conversion
    // -------------------------------------------------------------------------

    @Test
    void toTime_secondsSinceMidnight_convertsCorrectly() {
        // 45492.984 = 12:38:12 (verified via Query Service)
        Time result = EpochConverter.toTime("45492.984000000");
        assertEquals(Time.valueOf("12:38:12"), result);
    }

    @Test
    void toTime_zero_returnsMidnight() {
        // Keboola UI test: "0" → "00:00:00"
        assertEquals(Time.valueOf("00:00:00"), EpochConverter.toTime("0"));
    }

    @Test
    void toTime_endOfDay_returns235959() {
        // Keboola UI test: "86399" → "23:59:59"
        assertEquals(Time.valueOf("23:59:59"), EpochConverter.toTime("86399"));
    }

    @Test
    void toTime_82275_matches_ui_test() {
        // Keboola UI test: "82275.415" → "22:51:15"
        assertEquals(Time.valueOf("22:51:15"), EpochConverter.toTime("82275.415"));
    }

    @Test
    void toTime_3661_matches_ui_test() {
        // Keboola UI test: "3661" → "01:01:01"
        assertEquals(Time.valueOf("01:01:01"), EpochConverter.toTime("3661"));
    }

    @Test
    void toTime_null_returnsNull() {
        assertNull(EpochConverter.toTime(null));
    }

    @Test
    void formatTime_convertsToHHmmss() {
        assertEquals("12:38:12", EpochConverter.formatTime("45492.984000000"));
        assertEquals("00:00", EpochConverter.formatTime("0"));
        assertEquals("23:59:59", EpochConverter.formatTime("86399"));
    }

    @Test
    void formatTime_invalidInput_returnsOriginal() {
        assertEquals("invalid", EpochConverter.formatTime("invalid"));
    }

    // -------------------------------------------------------------------------
    // formatValue (generic)
    // -------------------------------------------------------------------------

    @Test
    void formatValue_dateType_formatsAsIsoDate() {
        assertEquals("2026-04-08", EpochConverter.formatValue("20551", "date"));
    }

    @Test
    void formatValue_timestampLtz_formatsWithZ() {
        String result = EpochConverter.formatValue("0", "timestamp_ltz");
        assertTrue(result.endsWith("Z"));
    }

    @Test
    void formatValue_timestampNtz_formatsWithoutZ() {
        String result = EpochConverter.formatValue("0", "timestamp_ntz");
        assertFalse(result.endsWith("Z"));
    }

    @Test
    void formatValue_timeType_formatsAsHHmmss() {
        assertEquals("12:38:12", EpochConverter.formatValue("45492.984000000", "time"));
    }

    @Test
    void formatValue_varchar_returnsAsIs() {
        assertEquals("hello", EpochConverter.formatValue("hello", "text"));
    }

    @Test
    void formatValue_nullValue_returnsNull() {
        assertNull(EpochConverter.formatValue(null, "date"));
    }

    @Test
    void formatValue_nullType_returnsOriginal() {
        assertEquals("20551", EpochConverter.formatValue("20551", null));
    }
}
