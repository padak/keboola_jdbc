/**
 * Converts Snowflake epoch-based values returned by the Query Service /results
 * endpoint into human-readable strings.
 *
 * The Query Service returns raw Snowflake internal representations:
 * - DATE — days since 1970-01-01 (e.g. "20551" = 2026-04-08)
 * - TIMESTAMP_* — seconds.nanoseconds since Unix epoch (e.g. "1775677092.984000000")
 * - TIME — seconds.nanoseconds since midnight (e.g. "45492.984000000" = 12:38:12)
 *
 * Logic mirrors the Keboola UI implementation in kbc-ui/.../QueryResults/helpers.ts.
 *
 * @see https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses
 */

const SECONDS_IN_DAY = 86400;

const DATE_TYPES = new Set(['date']);
const TIMESTAMP_TYPES = new Set([
  'timestamp_tz',
  'timestamp_ltz',
  'timestamp_ntz',
  'datetime',
  'timestamp',
]);
const TIME_TYPES = new Set(['time']);
const TZ_TYPES = new Set(['timestamp_tz', 'timestamp_ltz']);

function pad(n: number, width = 2): string {
  return String(n).padStart(width, '0');
}

function formatIsoDate(d: Date): string {
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
}

function formatIsoTimestamp(d: Date, withTz: boolean): string {
  const date = formatIsoDate(d);
  const time = `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
  return `${date}T${time}${withTz ? 'Z' : ''}`;
}

function formatTimeOfDay(totalSeconds: number): string {
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = Math.floor(totalSeconds % 60);
  return `${pad(hours)}:${pad(minutes)}:${pad(seconds)}`;
}

/**
 * Converts a raw value from the Query Service to a human-readable string
 * based on the Snowflake column type.
 *
 * @param value - Raw string value from the API
 * @param type - Snowflake column type (e.g. "date", "timestamp_ltz", "time")
 * @returns Formatted string, or original value if conversion fails or type is not date/time
 */
export function prepareValue(value: string | null | undefined, type?: string): string | null | undefined {
  if (value == null || !type) return value;

  const normalizedType = type.toLowerCase();

  try {
    if (DATE_TYPES.has(normalizedType)) {
      const epochDays = parseInt(value, 10);
      if (isNaN(epochDays)) return value;
      const d = new Date(epochDays * SECONDS_IN_DAY * 1000);
      return formatIsoDate(d);
    }

    if (TIMESTAMP_TYPES.has(normalizedType)) {
      // Strip TIMESTAMP_TZ offset (space-separated minutes)
      const numericPart = value.split(' ')[0];
      const epochSeconds = parseFloat(numericPart);
      if (isNaN(epochSeconds)) return value;
      const d = new Date(epochSeconds * 1000);
      return formatIsoTimestamp(d, TZ_TYPES.has(normalizedType));
    }

    if (TIME_TYPES.has(normalizedType)) {
      const totalSeconds = parseFloat(value);
      if (isNaN(totalSeconds)) return value;
      return formatTimeOfDay(totalSeconds);
    }
  } catch {
    return value;
  }

  return value;
}
