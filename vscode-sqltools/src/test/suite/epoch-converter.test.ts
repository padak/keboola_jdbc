import * as assert from 'assert';
import { prepareValue } from '../../ls/epoch-converter';

suite('EpochConverter - prepareValue', () => {
  // -------------------------------------------------------------------------
  // DATE type
  // -------------------------------------------------------------------------

  suite('DATE type', () => {
    test('converts epoch days to ISO date', () => {
      // 20551 days = 2026-04-08 (verified via Query Service)
      assert.strictEqual(prepareValue('20551', 'date'), '2026-04-08');
    });

    test('converts epoch days (Keboola UI test: 20398 → 2025-11-06)', () => {
      assert.strictEqual(prepareValue('20398', 'DATE'), '2025-11-06');
    });

    test('handles zero (epoch start)', () => {
      assert.strictEqual(prepareValue('0', 'date'), '1970-01-01');
    });

    test('handles negative days (before epoch)', () => {
      assert.strictEqual(prepareValue('-1', 'date'), '1969-12-31');
    });

    test('is case-insensitive', () => {
      assert.strictEqual(prepareValue('20551', 'Date'), '2026-04-08');
    });

    test('returns original for non-numeric value', () => {
      assert.strictEqual(prepareValue('invalid', 'date'), 'invalid');
    });
  });

  // -------------------------------------------------------------------------
  // TIME type
  // -------------------------------------------------------------------------

  suite('TIME type', () => {
    test('converts seconds since midnight', () => {
      // 45492.984 = 12:38:12 (verified via Query Service)
      assert.strictEqual(prepareValue('45492.984000000', 'time'), '12:38:12');
    });

    test('handles midnight', () => {
      assert.strictEqual(prepareValue('0', 'time'), '00:00:00');
    });

    test('handles end of day', () => {
      // Keboola UI test: 86399 → 23:59:59
      assert.strictEqual(prepareValue('86399', 'time'), '23:59:59');
    });

    test('converts 82275.415 (Keboola UI test → 22:51:15)', () => {
      assert.strictEqual(prepareValue('82275.415', 'TIME'), '22:51:15');
    });

    test('converts 3661 (Keboola UI test → 01:01:01)', () => {
      assert.strictEqual(prepareValue('3661', 'Time'), '01:01:01');
    });

    test('returns original for non-numeric value', () => {
      assert.strictEqual(prepareValue('invalid', 'time'), 'invalid');
    });
  });

  // -------------------------------------------------------------------------
  // TIMESTAMP types
  // -------------------------------------------------------------------------

  suite('TIMESTAMP_TZ type', () => {
    test('converts epoch seconds with Z suffix', () => {
      const result = prepareValue('1762498275', 'TIMESTAMP_TZ');
      assert.ok(result!.endsWith('Z'), `Expected Z suffix, got: ${result}`);
      assert.ok(result!.includes('T'), `Expected ISO format, got: ${result}`);
    });

    test('handles epoch zero', () => {
      assert.strictEqual(prepareValue('0', 'timestamp_tz'), '1970-01-01T00:00:00Z');
    });

    test('handles fractional seconds', () => {
      const result = prepareValue('1762498275.415', 'timestamp_tz');
      assert.ok(result!.endsWith('Z'));
    });

    test('strips TIMESTAMP_TZ offset', () => {
      // Snowflake TIMESTAMP_TZ format: "seconds.nanos offset_minutes"
      const result = prepareValue('1762498275.000000000 1560', 'timestamp_tz');
      assert.ok(result!.endsWith('Z'));
    });
  });

  suite('TIMESTAMP_LTZ type', () => {
    test('converts with Z suffix (same as TZ)', () => {
      const result = prepareValue('1762498275', 'TIMESTAMP_LTZ');
      assert.ok(result!.endsWith('Z'));
    });
  });

  suite('TIMESTAMP_NTZ type', () => {
    test('converts without Z suffix', () => {
      const result = prepareValue('1762498275', 'TIMESTAMP_NTZ');
      assert.ok(!result!.endsWith('Z'), `Expected no Z, got: ${result}`);
      assert.ok(result!.includes('T'));
    });
  });

  suite('DATETIME type', () => {
    test('converts as alias for TIMESTAMP_NTZ', () => {
      const result = prepareValue('1762498275', 'DATETIME');
      assert.ok(!result!.endsWith('Z'));
    });
  });

  // -------------------------------------------------------------------------
  // Edge cases
  // -------------------------------------------------------------------------

  suite('edge cases', () => {
    test('returns null for null value', () => {
      assert.strictEqual(prepareValue(null, 'date'), null);
    });

    test('returns undefined for undefined value', () => {
      assert.strictEqual(prepareValue(undefined, 'date'), undefined);
    });

    test('returns original for no type', () => {
      assert.strictEqual(prepareValue('12345'), '12345');
    });

    test('returns original for unknown type', () => {
      assert.strictEqual(prepareValue('hello', 'varchar'), 'hello');
    });

    test('returns original for text type', () => {
      assert.strictEqual(prepareValue('2025-03-24', 'text'), '2025-03-24');
    });
  });
});
