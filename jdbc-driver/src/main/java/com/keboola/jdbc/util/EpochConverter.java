package com.keboola.jdbc.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * Converts Snowflake epoch-based values returned by the Query Service {@code /results}
 * endpoint into standard Java SQL date/time types.
 *
 * <p>The Query Service returns raw Snowflake internal representations:
 * <ul>
 *   <li><b>DATE</b> — days since 1970-01-01 (e.g. {@code "20551"} = 2026-04-08)</li>
 *   <li><b>TIMESTAMP_*</b> — seconds.nanoseconds since Unix epoch
 *       (e.g. {@code "1775677092.984000000"})</li>
 *   <li><b>TIME</b> — seconds.nanoseconds since midnight
 *       (e.g. {@code "45492.984000000"} = 12:38:12.984)</li>
 * </ul>
 *
 * <p>Conversion logic mirrors the Keboola UI implementation in
 * {@code kbc-ui/.../QueryResults/helpers.ts}.
 *
 * @see <a href="https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses">
 *     Snowflake SQL API: Handling Responses</a>
 */
public final class EpochConverter {

    private EpochConverter() {}

    // -------------------------------------------------------------------------
    // Type detection
    // -------------------------------------------------------------------------

    /**
     * Returns true if the given Snowflake type name represents a DATE column.
     */
    public static boolean isDateType(String snowflakeType) {
        if (snowflakeType == null) return false;
        return snowflakeType.equalsIgnoreCase("date");
    }

    /**
     * Returns true if the given Snowflake type name represents a TIMESTAMP column.
     */
    public static boolean isTimestampType(String snowflakeType) {
        if (snowflakeType == null) return false;
        String t = snowflakeType.toLowerCase();
        return t.equals("timestamp_tz") || t.equals("timestamp_ltz")
                || t.equals("timestamp_ntz") || t.equals("datetime")
                || t.equals("timestamp");
    }

    /**
     * Returns true if the given Snowflake type name represents a TIME column.
     */
    public static boolean isTimeType(String snowflakeType) {
        if (snowflakeType == null) return false;
        return snowflakeType.equalsIgnoreCase("time");
    }

    /**
     * Returns true if the given Snowflake type name is any date/time type
     * that requires epoch conversion.
     */
    public static boolean isDateTimeType(String snowflakeType) {
        return isDateType(snowflakeType) || isTimestampType(snowflakeType)
                || isTimeType(snowflakeType);
    }

    // -------------------------------------------------------------------------
    // DATE conversion: epoch days → java.sql.Date
    // -------------------------------------------------------------------------

    /**
     * Converts an epoch-days string to {@link java.sql.Date}.
     *
     * @param epochDaysStr string representation of days since 1970-01-01
     * @return the corresponding SQL Date, or null if the input is null
     * @throws NumberFormatException if the string is not a valid integer
     */
    public static Date toDate(String epochDaysStr) {
        if (epochDaysStr == null) return null;
        long epochDays = Long.parseLong(epochDaysStr.trim());
        return Date.valueOf(LocalDate.ofEpochDay(epochDays));
    }

    /**
     * Formats an epoch-days string as an ISO date string (YYYY-MM-DD).
     *
     * @param epochDaysStr string representation of days since 1970-01-01
     * @return formatted date string, or the original value if conversion fails
     */
    public static String formatDate(String epochDaysStr) {
        if (epochDaysStr == null) return null;
        try {
            long epochDays = Long.parseLong(epochDaysStr.trim());
            return LocalDate.ofEpochDay(epochDays).toString();
        } catch (Exception e) {
            return epochDaysStr;
        }
    }

    // -------------------------------------------------------------------------
    // TIMESTAMP conversion: epoch seconds.nanos → java.sql.Timestamp
    // -------------------------------------------------------------------------

    /**
     * Converts an epoch-seconds string (with optional nanoseconds) to {@link Timestamp}.
     *
     * <p>The input format is {@code "seconds.nanoseconds"} (e.g. {@code "1775677092.984000000"}).
     * For TIMESTAMP_TZ, the Snowflake format may include a space-separated timezone offset
     * in minutes (e.g. {@code "1775677092.984000000 1560"}), which is parsed and applied.
     *
     * @param epochSecondsStr string representation of seconds since Unix epoch
     * @return the corresponding Timestamp, or null if the input is null
     * @throws NumberFormatException if the string is not a valid number
     */
    public static Timestamp toTimestamp(String epochSecondsStr) {
        if (epochSecondsStr == null) return null;
        String trimmed = epochSecondsStr.trim();
        // Strip TIMESTAMP_TZ offset (space-separated minutes), e.g. "1234567890.000 1560"
        int spaceIdx = trimmed.indexOf(' ');
        if (spaceIdx > 0) {
            trimmed = trimmed.substring(0, spaceIdx);
        }
        BigDecimal bd = new BigDecimal(trimmed);
        long epochSeconds = bd.longValue();
        int nanos = bd.subtract(BigDecimal.valueOf(epochSeconds))
                .movePointRight(9)
                .intValue();
        Instant instant = Instant.ofEpochSecond(epochSeconds, nanos);
        return Timestamp.from(instant);
    }

    /**
     * Formats an epoch-seconds string as an ISO timestamp string.
     *
     * @param epochSecondsStr string representation of seconds since Unix epoch
     * @param withTimezone    if true, appends "Z" suffix
     * @return formatted timestamp string, or the original value if conversion fails
     */
    public static String formatTimestamp(String epochSecondsStr, boolean withTimezone) {
        if (epochSecondsStr == null) return null;
        try {
            Timestamp ts = toTimestamp(epochSecondsStr);
            // Format as ISO: YYYY-MM-DDTHH:mm:ss[Z]
            Instant instant = ts.toInstant();
            String formatted = instant.toString(); // ISO-8601 with Z
            if (!withTimezone) {
                // Remove trailing Z for NTZ
                if (formatted.endsWith("Z")) {
                    formatted = formatted.substring(0, formatted.length() - 1);
                }
            }
            return formatted;
        } catch (Exception e) {
            return epochSecondsStr;
        }
    }

    // -------------------------------------------------------------------------
    // TIME conversion: seconds since midnight → java.sql.Time
    // -------------------------------------------------------------------------

    /**
     * Converts a seconds-since-midnight string to {@link Time}.
     *
     * @param secondsStr string representation of seconds since midnight
     *                   (e.g. {@code "45492.984000000"})
     * @return the corresponding SQL Time, or null if the input is null
     * @throws NumberFormatException if the string is not a valid number
     */
    public static Time toTime(String secondsStr) {
        if (secondsStr == null) return null;
        BigDecimal bd = new BigDecimal(secondsStr.trim());
        int totalSeconds = bd.intValue();
        int nanos = bd.subtract(BigDecimal.valueOf(totalSeconds))
                .movePointRight(9)
                .intValue();
        LocalTime lt = LocalTime.ofSecondOfDay(totalSeconds).withNano(nanos);
        return Time.valueOf(lt);
    }

    /**
     * Formats a seconds-since-midnight string as HH:mm:ss.
     *
     * @param secondsStr string representation of seconds since midnight
     * @return formatted time string, or the original value if conversion fails
     */
    public static String formatTime(String secondsStr) {
        if (secondsStr == null) return null;
        try {
            BigDecimal bd = new BigDecimal(secondsStr.trim());
            int totalSeconds = bd.intValue();
            LocalTime lt = LocalTime.ofSecondOfDay(totalSeconds);
            return lt.toString(); // HH:mm:ss
        } catch (Exception e) {
            return secondsStr;
        }
    }

    // -------------------------------------------------------------------------
    // Generic string formatting based on column type
    // -------------------------------------------------------------------------

    /**
     * Formats a raw value from the Query Service as a human-readable string,
     * based on the Snowflake column type. Non-date/time types are returned as-is.
     *
     * @param rawValue      the raw string value from the API
     * @param snowflakeType the Snowflake column type name (e.g. "date", "timestamp_ltz")
     * @return the formatted string
     */
    public static String formatValue(String rawValue, String snowflakeType) {
        if (rawValue == null || snowflakeType == null) return rawValue;
        if (isDateType(snowflakeType)) {
            return formatDate(rawValue);
        }
        if (isTimestampType(snowflakeType)) {
            boolean withTz = snowflakeType.equalsIgnoreCase("timestamp_tz")
                    || snowflakeType.equalsIgnoreCase("timestamp_ltz");
            return formatTimestamp(rawValue, withTz);
        }
        if (isTimeType(snowflakeType)) {
            return formatTime(rawValue);
        }
        return rawValue;
    }
}
