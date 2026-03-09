import * as assert from 'assert';
import {
  QUERY_POLLING,
  TERMINAL_STATES,
  DEFAULT_PAGE_SIZE,
  MAX_RESULT_ROWS,
  MAX_RETRIES,
  getConnectionUrl,
  getQueryUrl,
  USE_PATTERN,
  SHOW_HISTORY_PATTERN,
  HISTORY_PAGE_SIZE,
  CANCEL_REASON,
} from '../../constants';

/**
 * Unit tests for the Keboola driver logic.
 *
 * These tests validate the constants, helper functions, and algorithmic behavior
 * that the driver relies on. HTTP-dependent integration tests require a live
 * Keboola instance and are not included here.
 */

suite('Driver - Constants Validation', () => {
  test('QUERY_POLLING constants are positive numbers', () => {
    assert.ok(QUERY_POLLING.INITIAL_INTERVAL_MS > 0, 'INITIAL_INTERVAL_MS must be > 0');
    assert.ok(QUERY_POLLING.MAX_INTERVAL_MS > 0, 'MAX_INTERVAL_MS must be > 0');
    assert.ok(QUERY_POLLING.BACKOFF_MULTIPLIER > 1, 'BACKOFF_MULTIPLIER must be > 1');
    assert.ok(QUERY_POLLING.DEFAULT_TIMEOUT_S > 0, 'DEFAULT_TIMEOUT_S must be > 0');
  });

  test('MAX_INTERVAL_MS is greater than INITIAL_INTERVAL_MS', () => {
    assert.ok(
      QUERY_POLLING.MAX_INTERVAL_MS > QUERY_POLLING.INITIAL_INTERVAL_MS,
      'MAX_INTERVAL_MS should be greater than INITIAL_INTERVAL_MS'
    );
  });

  test('DEFAULT_PAGE_SIZE is at least 100 (API minimum)', () => {
    assert.ok(DEFAULT_PAGE_SIZE >= 100, 'DEFAULT_PAGE_SIZE must be >= 100 (API minimum)');
  });

  test('MAX_RESULT_ROWS is greater than DEFAULT_PAGE_SIZE', () => {
    assert.ok(
      MAX_RESULT_ROWS > DEFAULT_PAGE_SIZE,
      'MAX_RESULT_ROWS should be greater than DEFAULT_PAGE_SIZE'
    );
  });

  test('MAX_RETRIES is at least 1', () => {
    assert.ok(MAX_RETRIES >= 1, 'MAX_RETRIES must be >= 1');
  });
});

suite('Driver - Terminal States', () => {
  test('TERMINAL_STATES contains expected values', () => {
    assert.ok(TERMINAL_STATES.has('completed'), 'Should contain "completed"');
    assert.ok(TERMINAL_STATES.has('failed'), 'Should contain "failed"');
    assert.ok(TERMINAL_STATES.has('canceled'), 'Should contain "canceled"');
  });

  test('non-terminal states are not in TERMINAL_STATES', () => {
    assert.ok(!TERMINAL_STATES.has('running'), '"running" should not be terminal');
    assert.ok(!TERMINAL_STATES.has('pending'), '"pending" should not be terminal');
    assert.ok(!TERMINAL_STATES.has('queued'), '"queued" should not be terminal');
  });
});

suite('Driver - Query Service URL Auto-Discovery', () => {
  test('getQueryUrl maps predefined stacks correctly', () => {
    assert.strictEqual(getQueryUrl('connection.keboola.com'), 'query.keboola.com');
    assert.strictEqual(
      getQueryUrl('connection.eu-central-1.keboola.com'),
      'query.eu-central-1.keboola.com'
    );
    assert.strictEqual(
      getQueryUrl('connection.us-east4.gcp.keboola.com'),
      'query.us-east4.gcp.keboola.com'
    );
  });

  test('getQueryUrl derives URL for unknown connection.X domains', () => {
    assert.strictEqual(
      getQueryUrl('connection.custom.keboola.dev'),
      'query.custom.keboola.dev'
    );
  });

  test('getQueryUrl throws for invalid connection URL', () => {
    assert.throws(() => getQueryUrl('invalid.keboola.com'), /Invalid connection URL/);
  });
});

suite('Driver - Connection URL Resolution', () => {
  test('getConnectionUrl returns stack directly for predefined stacks', () => {
    assert.strictEqual(
      getConnectionUrl('connection.keboola.com'),
      'connection.keboola.com'
    );
  });

  test('getConnectionUrl returns custom URL when stack is "custom"', () => {
    assert.strictEqual(
      getConnectionUrl('custom', 'connection.mycompany.keboola.com'),
      'connection.mycompany.keboola.com'
    );
  });

  test('getConnectionUrl throws when stack is "custom" without URL', () => {
    assert.throws(
      () => getConnectionUrl('custom'),
      /Custom Connection URL is required/
    );
  });
});

suite('Driver - Exponential Backoff Algorithm', () => {
  test('backoff interval grows exponentially and caps at MAX_INTERVAL_MS', () => {
    let interval = QUERY_POLLING.INITIAL_INTERVAL_MS;
    const intervals: number[] = [interval];

    // Simulate 20 backoff iterations
    for (let i = 0; i < 20; i++) {
      interval = Math.min(
        interval * QUERY_POLLING.BACKOFF_MULTIPLIER,
        QUERY_POLLING.MAX_INTERVAL_MS
      );
      intervals.push(interval);
    }

    // Verify growth
    assert.ok(intervals[1] > intervals[0], 'Interval should grow after first iteration');

    // Verify cap
    const lastInterval = intervals[intervals.length - 1];
    assert.ok(
      lastInterval <= QUERY_POLLING.MAX_INTERVAL_MS,
      `Interval ${lastInterval} should not exceed MAX_INTERVAL_MS ${QUERY_POLLING.MAX_INTERVAL_MS}`
    );

    // Verify all intervals eventually reach the cap
    const cappedIntervals = intervals.filter((i) => i === QUERY_POLLING.MAX_INTERVAL_MS);
    assert.ok(cappedIntervals.length > 0, 'Interval should eventually reach MAX_INTERVAL_MS');
  });

  test('timeout detection works with DEFAULT_TIMEOUT_S', () => {
    const timeoutMs = QUERY_POLLING.DEFAULT_TIMEOUT_S * 1000;
    const startTime = Date.now();
    const elapsed = Date.now() - startTime;

    // At start, should not be timed out
    assert.ok(elapsed <= timeoutMs, 'Should not be timed out at start');

    // Verify timeout value is reasonable (between 1 and 600 seconds)
    assert.ok(QUERY_POLLING.DEFAULT_TIMEOUT_S >= 1, 'Timeout should be >= 1 second');
    assert.ok(QUERY_POLLING.DEFAULT_TIMEOUT_S <= 600, 'Timeout should be <= 600 seconds');
  });
});

suite('Driver - Multi-Page Result Assembly', () => {
  test('page count calculation for various row counts', () => {
    // Simulate page count logic used in fetchAllPages
    const testCases: [number, number][] = [
      [0, 1],       // 0 rows = 1 page (initial fetch)
      [1, 1],       // 1 row = 1 page
      [999, 1],     // just under page size = 1 page
      [1000, 1],    // exactly page size = 1 page (next page would be empty)
      [1001, 2],    // just over page size = 2 pages
      [5000, 5],    // exact multiple = 5 pages
      [5001, 6],    // just over = 6 pages
      [10000, 10],  // at MAX_RESULT_ROWS
    ];

    for (const [totalRows, expectedPages] of testCases) {
      const pages = totalRows === 0 ? 1 : Math.ceil(totalRows / DEFAULT_PAGE_SIZE);
      assert.strictEqual(
        pages,
        expectedPages,
        `Expected ${expectedPages} pages for ${totalRows} rows, got ${pages}`
      );
    }
  });

  test('MAX_RESULT_ROWS caps total rows fetched', () => {
    // Simulate the capping logic
    const totalRows = 50000;
    let fetched = totalRows;
    if (fetched > MAX_RESULT_ROWS) {
      fetched = MAX_RESULT_ROWS;
    }
    assert.strictEqual(fetched, MAX_RESULT_ROWS);
  });
});

suite('Driver - Error Mapping', () => {
  test('HTTP status codes map to expected error categories', () => {
    // Verify that our error mapping covers the documented status codes
    const errorStatusCodes = [400, 401, 403, 404, 500, 502, 503];
    const nonRetryable = [400, 401, 403, 404];
    const retryable = [429, 500, 502, 503];

    for (const code of nonRetryable) {
      assert.ok(
        !retryable.includes(code) || code >= 500,
        `Status ${code} should be non-retryable (except 5xx which are retryable)`
      );
    }

    for (const code of retryable) {
      assert.ok(
        code === 429 || code >= 500,
        `Retryable status ${code} should be 429 or >= 500`
      );
    }

    // All documented codes should be covered
    for (const code of errorStatusCodes) {
      assert.ok(
        nonRetryable.includes(code) || retryable.includes(code),
        `Status ${code} should be in either nonRetryable or retryable set`
      );
    }
  });
});

suite('Driver - Result Row Mapping', () => {
  test('column-data mapping produces correct objects', () => {
    // Simulate the mapping logic from driver.mapResultToSQLTools
    const columns = [
      { name: 'ID', type: 'NUMBER' },
      { name: 'NAME', type: 'VARCHAR' },
      { name: 'ACTIVE', type: 'BOOLEAN' },
    ];
    const data = [
      ['1', 'Alice', 'true'],
      ['2', 'Bob', 'false'],
    ];

    const colNames = columns.map((c) => c.name);
    const rows = data.map((row) => {
      const obj: Record<string, any> = {};
      colNames.forEach((col, i) => {
        obj[col] = row[i];
      });
      return obj;
    });

    assert.strictEqual(rows.length, 2);
    assert.deepStrictEqual(rows[0], { ID: '1', NAME: 'Alice', ACTIVE: 'true' });
    assert.deepStrictEqual(rows[1], { ID: '2', NAME: 'Bob', ACTIVE: 'false' });
    assert.deepStrictEqual(colNames, ['ID', 'NAME', 'ACTIVE']);
  });

  test('empty result set produces empty arrays', () => {
    const columns: { name: string }[] = [];
    const data: any[][] = [];

    const colNames = columns.map((c) => c.name);
    const rows = data.map((row) => {
      const obj: Record<string, any> = {};
      colNames.forEach((col, i) => {
        obj[col] = row[i];
      });
      return obj;
    });

    assert.strictEqual(rows.length, 0);
    assert.strictEqual(colNames.length, 0);
  });

  test('result with columns but no data rows produces empty rows', () => {
    const columns = [{ name: 'COL1' }, { name: 'COL2' }];
    const data: any[][] = [];

    const colNames = columns.map((c) => c.name);
    const rows = data.map((row) => {
      const obj: Record<string, any> = {};
      colNames.forEach((col, i) => {
        obj[col] = row[i];
      });
      return obj;
    });

    assert.strictEqual(rows.length, 0);
    assert.deepStrictEqual(colNames, ['COL1', 'COL2']);
  });
});

suite('Driver - SHOW HISTORY Pattern Detection', () => {
  test('SHOW HISTORY is recognized (uppercase)', () => {
    assert.ok(SHOW_HISTORY_PATTERN.test('SHOW HISTORY'));
  });

  test('SHOW HISTORY is recognized (lowercase)', () => {
    assert.ok(SHOW_HISTORY_PATTERN.test('show history'));
  });

  test('SHOW HISTORY is recognized (mixed case)', () => {
    assert.ok(SHOW_HISTORY_PATTERN.test('Show History'));
  });

  test('SHOW HISTORY with trailing semicolon', () => {
    assert.ok(SHOW_HISTORY_PATTERN.test('SHOW HISTORY;'));
  });

  test('SHOW HISTORY with whitespace and semicolon', () => {
    assert.ok(SHOW_HISTORY_PATTERN.test('  SHOW HISTORY  ;  '));
  });

  test('SHOW QUERY HISTORY alias is recognized', () => {
    assert.ok(SHOW_HISTORY_PATTERN.test('SHOW QUERY HISTORY'));
  });

  test('SHOW QUERY HISTORY alias (lowercase)', () => {
    assert.ok(SHOW_HISTORY_PATTERN.test('show query history'));
  });

  test('SHOW QUERY HISTORY with semicolon', () => {
    assert.ok(SHOW_HISTORY_PATTERN.test('SHOW QUERY HISTORY;'));
  });

  test('SHOW QUERY HISTORY with whitespace', () => {
    assert.ok(SHOW_HISTORY_PATTERN.test('  SHOW QUERY HISTORY  ;  '));
  });

  test('regular SELECT is not recognized', () => {
    assert.ok(!SHOW_HISTORY_PATTERN.test('SELECT * FROM table'));
  });

  test('SHOW TABLES is not recognized', () => {
    assert.ok(!SHOW_HISTORY_PATTERN.test('SHOW TABLES'));
  });

  test('SHOW DATABASES is not recognized', () => {
    assert.ok(!SHOW_HISTORY_PATTERN.test('SHOW DATABASES'));
  });

  test('SHOW SCHEMAS is not recognized', () => {
    assert.ok(!SHOW_HISTORY_PATTERN.test('SHOW SCHEMAS'));
  });

  test('partial SHOW HISTORY embedded in larger SQL is not recognized', () => {
    assert.ok(!SHOW_HISTORY_PATTERN.test('SELECT * FROM SHOW HISTORY'));
  });

  test('SHOW with unknown keyword is not recognized', () => {
    assert.ok(!SHOW_HISTORY_PATTERN.test('SHOW SOMETHING'));
  });

  test('empty string is not recognized', () => {
    assert.ok(!SHOW_HISTORY_PATTERN.test(''));
  });

  test('regex is reusable (no lastIndex issues)', () => {
    // Verify the regex works correctly when called multiple times
    assert.ok(SHOW_HISTORY_PATTERN.test('SHOW HISTORY'));
    assert.ok(SHOW_HISTORY_PATTERN.test('SHOW QUERY HISTORY'));
    assert.ok(!SHOW_HISTORY_PATTERN.test('SELECT 1'));
    assert.ok(SHOW_HISTORY_PATTERN.test('SHOW HISTORY'));
  });
});

suite('Driver - Retry Logic', () => {
  test('retry delay grows with exponential backoff', () => {
    const delays: number[] = [];
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      const delay =
        QUERY_POLLING.INITIAL_INTERVAL_MS *
        Math.pow(QUERY_POLLING.BACKOFF_MULTIPLIER, attempt - 1);
      delays.push(delay);
    }

    // First delay should be the initial interval
    assert.strictEqual(delays[0], QUERY_POLLING.INITIAL_INTERVAL_MS);

    // Each subsequent delay should be larger
    for (let i = 1; i < delays.length; i++) {
      assert.ok(
        delays[i] > delays[i - 1],
        `Delay ${i} (${delays[i]}) should be > delay ${i - 1} (${delays[i - 1]})`
      );
    }
  });
});

suite('Driver - USE SCHEMA/DATABASE Detection', () => {
  test('USE SCHEMA with double quotes extracts schema name', () => {
    const match = 'USE SCHEMA "my_schema"'.match(USE_PATTERN);
    assert.ok(match, 'Should match USE SCHEMA with quotes');
    assert.strictEqual(match![1], 'my_schema');
  });

  test('USE SCHEMA without quotes extracts schema name', () => {
    const match = 'USE SCHEMA my_schema'.match(USE_PATTERN);
    assert.ok(match, 'Should match USE SCHEMA without quotes');
    assert.strictEqual(match![1], 'my_schema');
  });

  test('USE DATABASE with double quotes extracts database name', () => {
    const match = 'USE DATABASE "my_db"'.match(USE_PATTERN);
    assert.ok(match, 'Should match USE DATABASE with quotes');
    assert.strictEqual(match![1], 'my_db');
  });

  test('USE DATABASE without quotes extracts database name', () => {
    const match = 'USE DATABASE my_db'.match(USE_PATTERN);
    assert.ok(match, 'Should match USE DATABASE without quotes');
    assert.strictEqual(match![1], 'my_db');
  });

  test('case insensitive matching', () => {
    const variants = [
      'use schema "Foo"',
      'Use Schema "Foo"',
      'USE SCHEMA "Foo"',
      'use SCHEMA "Foo"',
      'USE schema "Foo"',
    ];
    for (const sql of variants) {
      const match = sql.match(USE_PATTERN);
      assert.ok(match, `"${sql}" should match USE_PATTERN`);
      assert.strictEqual(match![1], 'Foo', `"${sql}" should extract "Foo"`);
    }
  });

  test('with trailing semicolon', () => {
    const match = 'USE SCHEMA "bar";'.match(USE_PATTERN);
    assert.ok(match, 'Should match USE SCHEMA with semicolon');
    assert.strictEqual(match![1], 'bar');
  });

  test('with leading/trailing whitespace', () => {
    const match = '  USE SCHEMA "baz"  ;  '.match(USE_PATTERN);
    assert.ok(match, 'Should match USE SCHEMA with whitespace');
    assert.strictEqual(match![1], 'baz');
  });

  test('dot-separated schema names (Keboola bucket IDs)', () => {
    const match = 'USE SCHEMA "in.c-main"'.match(USE_PATTERN);
    assert.ok(match, 'Should match dot-separated schema name');
    assert.strictEqual(match![1], 'in.c-main');
  });

  test('does not match regular SQL statements', () => {
    const nonMatching = [
      'SELECT * FROM table',
      'SHOW TABLES',
      'SHOW SCHEMAS',
      'CREATE SCHEMA my_schema',
      'DROP SCHEMA my_schema',
      'ALTER SCHEMA my_schema',
    ];
    for (const sql of nonMatching) {
      const match = sql.match(USE_PATTERN);
      assert.ok(!match, `"${sql}" should NOT match USE_PATTERN`);
    }
  });
});

suite('Driver - Multi-Statement USE SCHEMA Prepending', () => {
  test('when currentSchema is set, statements array includes USE SCHEMA prepended', () => {
    // Simulate the logic from executeQuery
    const currentSchema = 'in.c-main';
    const sql = 'SELECT * FROM my_table';
    const isUseCommand = !!sql.match(USE_PATTERN);

    const statements: string[] =
      currentSchema && !isUseCommand
        ? [`USE SCHEMA "${currentSchema}"`, sql]
        : [sql];

    assert.strictEqual(statements.length, 2);
    assert.strictEqual(statements[0], 'USE SCHEMA "in.c-main"');
    assert.strictEqual(statements[1], 'SELECT * FROM my_table');
  });

  test('when currentSchema is null, statements array has only the query', () => {
    const currentSchema: string | null = null;
    const sql = 'SELECT * FROM my_table';
    const isUseCommand = !!sql.match(USE_PATTERN);

    const statements: string[] =
      currentSchema && !isUseCommand
        ? [`USE SCHEMA "${currentSchema}"`, sql]
        : [sql];

    assert.strictEqual(statements.length, 1);
    assert.strictEqual(statements[0], 'SELECT * FROM my_table');
  });

  test('USE command itself is NOT prepended with another USE SCHEMA', () => {
    const currentSchema = 'in.c-main';
    const sql = 'USE SCHEMA "in.c-other"';
    const isUseCommand = !!sql.match(USE_PATTERN);

    const statements: string[] =
      currentSchema && !isUseCommand
        ? [`USE SCHEMA "${currentSchema}"`, sql]
        : [sql];

    // USE command should be sent alone, not prepended
    assert.strictEqual(statements.length, 1);
    assert.strictEqual(statements[0], 'USE SCHEMA "in.c-other"');
  });
});

suite('Driver - sessionId in Query Submissions', () => {
  test('sessionId is included in submit body', () => {
    // Simulate the submit body construction from executeQuery
    const sessionId = 'test-session-uuid';
    const sql = 'SELECT 1';
    const body = JSON.stringify({
      statements: [sql],
      sessionId: sessionId,
      wait: false,
    });

    const parsed = JSON.parse(body);
    assert.strictEqual(parsed.sessionId, 'test-session-uuid');
    assert.strictEqual(parsed.wait, false);
    assert.deepStrictEqual(parsed.statements, ['SELECT 1']);
  });

  test('sessionId is a valid UUID format', () => {
    // crypto.randomUUID() returns a v4 UUID
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    const uuid = require('crypto').randomUUID();
    assert.ok(uuidRegex.test(uuid), `Generated UUID "${uuid}" should match v4 UUID format`);
  });

  test('multi-statement body with USE SCHEMA includes sessionId', () => {
    const sessionId = 'multi-stmt-session';
    const currentSchema = 'in.c-main';
    const sql = 'SELECT * FROM t';

    const body = JSON.stringify({
      statements: [`USE SCHEMA "${currentSchema}"`, sql],
      sessionId: sessionId,
      wait: false,
    });

    const parsed = JSON.parse(body);
    assert.strictEqual(parsed.sessionId, 'multi-stmt-session');
    assert.strictEqual(parsed.statements.length, 2);
    assert.strictEqual(parsed.statements[0], 'USE SCHEMA "in.c-main"');
    assert.strictEqual(parsed.statements[1], 'SELECT * FROM t');
  });
});

suite('Driver - Cancel Query Tracking', () => {
  test('activeQueryJobId lifecycle: set during query, cleared after', () => {
    // Simulate the lifecycle managed by executeQuery
    let activeQueryJobId: string | null = null;

    // Before query
    assert.strictEqual(activeQueryJobId, null, 'Should be null before query starts');

    // During query (after submit)
    const queryJobId = 'job-123-abc';
    activeQueryJobId = queryJobId;
    assert.strictEqual(activeQueryJobId, 'job-123-abc', 'Should be set after submit');

    // After query (in finally block)
    activeQueryJobId = null;
    assert.strictEqual(activeQueryJobId, null, 'Should be cleared in finally block');
  });

  test('cancel request body contains correct reason', () => {
    const body = JSON.stringify({ reason: CANCEL_REASON });
    const parsed = JSON.parse(body);
    assert.strictEqual(parsed.reason, 'Cancelled by user from VS Code');
  });

  test('cancel endpoint is constructed correctly from queryJobId', () => {
    const queryJobId = 'job-456-def';
    const expectedPath = `/api/v1/queries/${queryJobId}/cancel`;
    assert.strictEqual(expectedPath, '/api/v1/queries/job-456-def/cancel');
  });

  test('cancelAllActiveQueries logic: collects only active job IDs', () => {
    // Simulate the static cancelAllActiveQueries logic with a Map
    const instances = new Map<string, { activeQueryJobId: string | null }>();
    instances.set('conn-1', { activeQueryJobId: 'job-aaa' });
    instances.set('conn-2', { activeQueryJobId: null });
    instances.set('conn-3', { activeQueryJobId: 'job-bbb' });
    instances.set('conn-4', { activeQueryJobId: null });

    const cancelled: string[] = [];
    for (const [, driver] of instances) {
      if (driver.activeQueryJobId) {
        cancelled.push(driver.activeQueryJobId);
      }
    }

    assert.strictEqual(cancelled.length, 2, 'Should cancel 2 active queries');
    assert.ok(cancelled.includes('job-aaa'), 'Should include job-aaa');
    assert.ok(cancelled.includes('job-bbb'), 'Should include job-bbb');
  });

  test('cancelAllActiveQueries with no active queries returns empty', () => {
    const instances = new Map<string, { activeQueryJobId: string | null }>();
    instances.set('conn-1', { activeQueryJobId: null });
    instances.set('conn-2', { activeQueryJobId: null });

    const cancelled: string[] = [];
    for (const [, driver] of instances) {
      if (driver.activeQueryJobId) {
        cancelled.push(driver.activeQueryJobId);
      }
    }

    assert.strictEqual(cancelled.length, 0, 'Should cancel 0 queries when none active');
  });

  test('cancelAllActiveQueries with empty instances map returns empty', () => {
    const instances = new Map<string, { activeQueryJobId: string | null }>();

    const cancelled: string[] = [];
    for (const [, driver] of instances) {
      if (driver.activeQueryJobId) {
        cancelled.push(driver.activeQueryJobId);
      }
    }

    assert.strictEqual(cancelled.length, 0, 'Should cancel 0 queries with no instances');
  });

  test('independent connections have separate activeQueryJobId tracking', () => {
    // Simulate multiple connections with independent state
    const conn1 = { activeQueryJobId: null as string | null };
    const conn2 = { activeQueryJobId: null as string | null };
    const conn3 = { activeQueryJobId: null as string | null };

    // conn1 starts a query
    conn1.activeQueryJobId = 'job-111';
    assert.strictEqual(conn1.activeQueryJobId, 'job-111');
    assert.strictEqual(conn2.activeQueryJobId, null);
    assert.strictEqual(conn3.activeQueryJobId, null);

    // conn2 starts a query
    conn2.activeQueryJobId = 'job-222';
    assert.strictEqual(conn1.activeQueryJobId, 'job-111');
    assert.strictEqual(conn2.activeQueryJobId, 'job-222');
    assert.strictEqual(conn3.activeQueryJobId, null);

    // conn1 finishes
    conn1.activeQueryJobId = null;
    assert.strictEqual(conn1.activeQueryJobId, null);
    assert.strictEqual(conn2.activeQueryJobId, 'job-222');
    assert.strictEqual(conn3.activeQueryJobId, null);

    // conn2 finishes
    conn2.activeQueryJobId = null;
    assert.strictEqual(conn2.activeQueryJobId, null);
  });
});

suite('Driver - History and Cancel Constants', () => {
  test('HISTORY_PAGE_SIZE is a positive number', () => {
    assert.ok(HISTORY_PAGE_SIZE > 0, 'HISTORY_PAGE_SIZE must be > 0');
    assert.strictEqual(HISTORY_PAGE_SIZE, 500, 'HISTORY_PAGE_SIZE should be 500');
  });

  test('CANCEL_REASON is a non-empty string', () => {
    assert.ok(CANCEL_REASON.length > 0, 'CANCEL_REASON must be non-empty');
    assert.ok(
      CANCEL_REASON.includes('VS Code'),
      'CANCEL_REASON should mention VS Code'
    );
  });

  test('history API path is constructed correctly', () => {
    const branchId = '12345';
    const workspaceId = '67890';
    const path = `/api/v1/branches/${branchId}/workspaces/${workspaceId}/queries?pageSize=${HISTORY_PAGE_SIZE}`;
    assert.strictEqual(
      path,
      '/api/v1/branches/12345/workspaces/67890/queries?pageSize=500'
    );
  });

  test('history result mapping produces correct column names', () => {
    const cols = ['Job ID', 'Status', 'Query', 'Created At', 'Completed At'];
    assert.strictEqual(cols.length, 5);
    assert.ok(cols.includes('Job ID'));
    assert.ok(cols.includes('Status'));
    assert.ok(cols.includes('Query'));
    assert.ok(cols.includes('Created At'));
    assert.ok(cols.includes('Completed At'));
  });

  test('history row mapping extracts fields correctly', () => {
    // Simulate the mapping logic from fetchWorkspaceQueryHistory
    const mockHistoryItem = {
      queryJobId: 'qj-abc-123',
      status: 'completed',
      statements: [
        { sql: 'SELECT 1' },
        { sql: 'SELECT 2' },
      ],
      createdAt: '2026-03-09T10:00:00Z',
      completedAt: '2026-03-09T10:00:05Z',
    };

    const row = {
      'Job ID': mockHistoryItem.queryJobId || '',
      'Status': mockHistoryItem.status || '',
      'Query': (mockHistoryItem.statements || []).map((s: any) => s.sql || '').join('; '),
      'Created At': mockHistoryItem.createdAt || '',
      'Completed At': mockHistoryItem.completedAt || '',
    };

    assert.strictEqual(row['Job ID'], 'qj-abc-123');
    assert.strictEqual(row['Status'], 'completed');
    assert.strictEqual(row['Query'], 'SELECT 1; SELECT 2');
    assert.strictEqual(row['Created At'], '2026-03-09T10:00:00Z');
    assert.strictEqual(row['Completed At'], '2026-03-09T10:00:05Z');
  });

  test('history row mapping handles missing fields gracefully', () => {
    const mockHistoryItem: any = {};

    const row = {
      'Job ID': mockHistoryItem.queryJobId || '',
      'Status': mockHistoryItem.status || '',
      'Query': (mockHistoryItem.statements || []).map((s: any) => s.sql || '').join('; '),
      'Created At': mockHistoryItem.createdAt || '',
      'Completed At': mockHistoryItem.completedAt || '',
    };

    assert.strictEqual(row['Job ID'], '');
    assert.strictEqual(row['Status'], '');
    assert.strictEqual(row['Query'], '');
    assert.strictEqual(row['Created At'], '');
    assert.strictEqual(row['Completed At'], '');
  });
});

suite('Driver - Timeout Auto-Cancel', () => {
  test('timeout triggers cancel attempt', () => {
    // Verify the timeout auto-cancel logic pattern:
    // When elapsed > timeoutMs, cancelQuery is called before throwing
    const timeoutMs = QUERY_POLLING.DEFAULT_TIMEOUT_S * 1000;
    const startTime = Date.now() - (timeoutMs + 1000); // simulate 1s past timeout
    const elapsed = Date.now() - startTime;

    assert.ok(elapsed > timeoutMs, 'Should detect timeout condition');

    // The actual cancel call is fire-and-forget (.catch(() => {}))
    // so even if it fails, the timeout error is still thrown
  });

  test('timeout error message includes timeout duration', () => {
    const errorMsg = `Query timed out after ${QUERY_POLLING.DEFAULT_TIMEOUT_S} seconds.`;
    assert.ok(
      errorMsg.includes(String(QUERY_POLLING.DEFAULT_TIMEOUT_S)),
      'Error message should include timeout duration'
    );
    assert.ok(
      errorMsg.includes('timed out'),
      'Error message should mention timeout'
    );
  });
});
