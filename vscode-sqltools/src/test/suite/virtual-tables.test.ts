import * as assert from 'assert';
import * as virtualTables from '../../ls/virtual-tables';
import * as helpCommand from '../../ls/help-command';

// -- Virtual Table Pattern Detection --

suite('VirtualTables - canHandle', () => {
  test('matches unquoted _keboola.components', () => {
    assert.strictEqual(
      virtualTables.canHandle('SELECT * FROM _keboola.components'),
      true
    );
  });

  test('matches unquoted _keboola.events', () => {
    assert.strictEqual(
      virtualTables.canHandle('SELECT * FROM _keboola.events'),
      true
    );
  });

  test('matches unquoted _keboola.tables', () => {
    assert.strictEqual(
      virtualTables.canHandle('SELECT * FROM _keboola.tables'),
      true
    );
  });

  test('matches unquoted _keboola.buckets', () => {
    assert.strictEqual(
      virtualTables.canHandle('SELECT * FROM _keboola.buckets'),
      true
    );
  });

  test('matches unquoted _keboola.jobs', () => {
    assert.strictEqual(
      virtualTables.canHandle('SELECT * FROM _keboola.jobs'),
      true
    );
  });

  test('matches double-quoted identifiers', () => {
    assert.strictEqual(
      virtualTables.canHandle('SELECT * FROM "_keboola"."events"'),
      true
    );
  });

  test('matches single-quoted identifiers', () => {
    assert.strictEqual(
      virtualTables.canHandle("SELECT * FROM '_keboola'.'events'"),
      true
    );
  });

  test('matches with LIMIT clause', () => {
    assert.strictEqual(
      virtualTables.canHandle('SELECT * FROM _keboola.components LIMIT 5'),
      true
    );
  });

  test('matches case-insensitive', () => {
    assert.strictEqual(
      virtualTables.canHandle('select * from _keboola.COMPONENTS'),
      true
    );
  });

  test('matches with specific columns', () => {
    assert.strictEqual(
      virtualTables.canHandle(
        'SELECT component_id, component_name FROM _keboola.components'
      ),
      true
    );
  });

  test('does not match regular SELECT without _keboola', () => {
    assert.strictEqual(
      virtualTables.canHandle('SELECT * FROM users WHERE id = 1'),
      false
    );
  });

  test('does not match unknown virtual table', () => {
    assert.strictEqual(
      virtualTables.canHandle('SELECT * FROM _keboola.unknown_table'),
      false
    );
  });

  test('does not match non-SELECT statements', () => {
    assert.strictEqual(
      virtualTables.canHandle('INSERT INTO _keboola.events VALUES (1)'),
      false
    );
  });

  test('does not match empty string', () => {
    assert.strictEqual(virtualTables.canHandle(''), false);
  });

  test('does not match partial _keboola reference without FROM', () => {
    assert.strictEqual(
      virtualTables.canHandle('SELECT _keboola.events'),
      false
    );
  });
});

// -- LIMIT Parsing --

suite('VirtualTables - parseLimitFromSql', () => {
  test('extracts explicit LIMIT 5', () => {
    assert.strictEqual(
      virtualTables.parseLimitFromSql('SELECT * FROM _keboola.events LIMIT 5'),
      5
    );
  });

  test('extracts explicit LIMIT 50', () => {
    assert.strictEqual(
      virtualTables.parseLimitFromSql(
        'SELECT * FROM _keboola.components LIMIT 50'
      ),
      50
    );
  });

  test('returns default when no LIMIT specified', () => {
    assert.strictEqual(
      virtualTables.parseLimitFromSql('SELECT * FROM _keboola.events'),
      100 // VIRTUAL_TABLE_DEFAULT_LIMIT
    );
  });

  test('extracts LIMIT 0', () => {
    assert.strictEqual(
      virtualTables.parseLimitFromSql(
        'SELECT * FROM _keboola.events LIMIT 0'
      ),
      0
    );
  });

  test('handles case-insensitive LIMIT', () => {
    assert.strictEqual(
      virtualTables.parseLimitFromSql(
        'SELECT * FROM _keboola.events limit 10'
      ),
      10
    );
  });

  test('extracts LIMIT with extra whitespace', () => {
    assert.strictEqual(
      virtualTables.parseLimitFromSql(
        'SELECT * FROM _keboola.events  LIMIT   25'
      ),
      25
    );
  });
});

// -- Virtual Table Execute --

suite('VirtualTables - execute', () => {
  const connId = 'test-conn-id';

  test('components: returns correct columns and rows', async () => {
    const mockApi = async (path: string) => {
      if (path === '/v2/storage/components') {
        return [
          {
            id: 'keboola.ex-db-snowflake',
            name: 'Snowflake Extractor',
            type: 'extractor',
            configurations: [
              {
                id: '12345',
                name: 'My Config',
                description: 'Test config',
                version: 3,
                created: '2024-01-15T10:00:00Z',
                isDisabled: false,
              },
            ],
          },
        ];
      }
      return [];
    };

    const results = await virtualTables.execute(
      'SELECT * FROM _keboola.components',
      connId,
      mockApi as any
    );

    assert.strictEqual(results.length, 1);
    const result = results[0];
    assert.strictEqual(result.cols.length, 9);
    assert.ok(result.cols.includes('component_id'));
    assert.ok(result.cols.includes('config_id'));
    assert.strictEqual(result.results.length, 1);
    assert.strictEqual(
      (result.results[0] as any).component_id,
      'keboola.ex-db-snowflake'
    );
    assert.strictEqual((result.results[0] as any).config_name, 'My Config');
    assert.strictEqual((result.results[0] as any).is_disabled, 'false');
  });

  test('events: returns correct columns and rows', async () => {
    const mockApi = async (path: string) => {
      if (path.startsWith('/v2/storage/events')) {
        return [
          {
            uuid: 'evt-001',
            type: 'info',
            component: 'keboola.runner',
            message: 'Job started',
            created: '2024-01-15T10:00:00Z',
          },
        ];
      }
      return [];
    };

    const results = await virtualTables.execute(
      'SELECT * FROM _keboola.events LIMIT 10',
      connId,
      mockApi as any
    );

    assert.strictEqual(results.length, 1);
    const result = results[0];
    assert.deepStrictEqual(result.cols, [
      'event_id',
      'type',
      'component',
      'message',
      'created',
    ]);
    assert.strictEqual(result.results.length, 1);
    assert.strictEqual((result.results[0] as any).event_id, 'evt-001');
  });

  test('tables: returns correct columns and rows', async () => {
    const mockApi = async (path: string) => {
      if (path.startsWith('/v2/storage/tables')) {
        return [
          {
            id: 'in.c-main.users',
            bucket: { id: 'in.c-main' },
            name: 'users',
            primaryKey: ['id'],
            rowsCount: 1000,
            dataSizeBytes: 50000,
            lastImportDate: '2024-01-15T10:00:00Z',
            created: '2024-01-01T00:00:00Z',
          },
        ];
      }
      return [];
    };

    const results = await virtualTables.execute(
      'SELECT * FROM _keboola.tables',
      connId,
      mockApi as any
    );

    assert.strictEqual(results.length, 1);
    const result = results[0];
    assert.strictEqual(result.cols.length, 8);
    assert.ok(result.cols.includes('table_id'));
    assert.ok(result.cols.includes('primary_key'));
    assert.strictEqual(result.results.length, 1);
    assert.strictEqual((result.results[0] as any).table_id, 'in.c-main.users');
    assert.strictEqual((result.results[0] as any).primary_key, 'id');
    assert.strictEqual((result.results[0] as any).rows_count, 1000);
  });

  test('buckets: returns correct columns and rows', async () => {
    const mockApi = async (path: string) => {
      if (path === '/v2/storage/buckets') {
        return [
          {
            id: 'in.c-main',
            name: 'c-main',
            stage: 'in',
            description: 'Main bucket',
            tables: [{}, {}, {}],
            dataSizeBytes: 150000,
            created: '2024-01-01T00:00:00Z',
          },
        ];
      }
      return [];
    };

    const results = await virtualTables.execute(
      'SELECT * FROM _keboola.buckets',
      connId,
      mockApi as any
    );

    assert.strictEqual(results.length, 1);
    const result = results[0];
    assert.deepStrictEqual(result.cols, [
      'bucket_id',
      'name',
      'stage',
      'description',
      'tables_count',
      'data_size_bytes',
      'created',
    ]);
    assert.strictEqual(result.results.length, 1);
    assert.strictEqual((result.results[0] as any).bucket_id, 'in.c-main');
    assert.strictEqual((result.results[0] as any).tables_count, 3);
  });

  test('jobs: returns correct columns and rows', async () => {
    const mockApi = async (path: string) => {
      if (path.startsWith('/v2/storage/jobs')) {
        return [
          {
            id: '99001',
            component: 'keboola.ex-db-snowflake',
            config: '12345',
            status: 'success',
            createdTime: '2024-01-15T10:00:00Z',
            startTime: '2024-01-15T10:00:05Z',
            endTime: '2024-01-15T10:01:00Z',
            durationSeconds: 55,
          },
        ];
      }
      return [];
    };

    const results = await virtualTables.execute(
      'SELECT * FROM _keboola.jobs LIMIT 5',
      connId,
      mockApi as any
    );

    assert.strictEqual(results.length, 1);
    const result = results[0];
    assert.strictEqual(result.cols.length, 8);
    assert.ok(result.cols.includes('job_id'));
    assert.ok(result.cols.includes('duration_sec'));
    assert.strictEqual(result.results.length, 1);
    assert.strictEqual((result.results[0] as any).job_id, '99001');
    assert.strictEqual((result.results[0] as any).duration_sec, 55);
  });

  test('double-quoted identifiers work', async () => {
    const mockApi = async () => [];

    const results = await virtualTables.execute(
      'SELECT * FROM "_keboola"."buckets"',
      connId,
      mockApi as any
    );

    assert.strictEqual(results.length, 1);
    assert.strictEqual(results[0].error, undefined);
  });

  test('API error returns error result', async () => {
    const mockApi = async () => {
      throw new Error('Connection refused');
    };

    const results = await virtualTables.execute(
      'SELECT * FROM _keboola.events',
      connId,
      mockApi as any
    );

    assert.strictEqual(results.length, 1);
    assert.strictEqual(results[0].error, true);
    assert.ok(
      (results[0].messages[0] as any).message.includes('Connection refused')
    );
  });

  test('LIMIT is applied to results', async () => {
    const mockApi = async (path: string) => {
      if (path === '/v2/storage/buckets') {
        return Array.from({ length: 10 }, (_, i) => ({
          id: `bucket-${i}`,
          name: `b-${i}`,
          stage: 'in',
          description: '',
          tables: [],
          dataSizeBytes: 0,
          created: '',
        }));
      }
      return [];
    };

    const results = await virtualTables.execute(
      'SELECT * FROM _keboola.buckets LIMIT 3',
      connId,
      mockApi as any
    );

    assert.strictEqual(results[0].results.length, 3);
  });
});

// -- Help Command --

suite('HelpCommand - canHandle', () => {
  test('matches KEBOOLA HELP', () => {
    assert.strictEqual(helpCommand.canHandle('KEBOOLA HELP'), true);
  });

  test('matches keboola help (case-insensitive)', () => {
    assert.strictEqual(helpCommand.canHandle('keboola help'), true);
  });

  test('matches with trailing semicolon', () => {
    assert.strictEqual(helpCommand.canHandle('KEBOOLA HELP;'), true);
  });

  test('matches with leading/trailing whitespace', () => {
    assert.strictEqual(helpCommand.canHandle('  KEBOOLA HELP  '), true);
  });

  test('matches with semicolon and whitespace', () => {
    assert.strictEqual(helpCommand.canHandle('  keboola help ; '), true);
  });

  test('does not match partial command', () => {
    assert.strictEqual(helpCommand.canHandle('KEBOOLA'), false);
  });

  test('does not match with extra words', () => {
    assert.strictEqual(helpCommand.canHandle('KEBOOLA HELP ME'), false);
  });

  test('does not match regular SQL', () => {
    assert.strictEqual(helpCommand.canHandle('SELECT * FROM users'), false);
  });

  test('does not match empty string', () => {
    assert.strictEqual(helpCommand.canHandle(''), false);
  });
});

suite('HelpCommand - execute', () => {
  test('returns result with correct columns', () => {
    const result = helpCommand.execute('test-conn');
    assert.deepStrictEqual(result.cols, ['command', 'syntax', 'description']);
  });

  test('returns all expected commands', () => {
    const result = helpCommand.execute('test-conn');
    const commands = result.results.map((r: any) => r.command);
    assert.ok(commands.includes('KEBOOLA HELP'));
    assert.ok(commands.includes('SHOW HISTORY'));
    assert.ok(commands.includes('USE SCHEMA'));
    assert.ok(commands.includes('USE DATABASE'));
    assert.ok(commands.includes('_keboola.components'));
    assert.ok(commands.includes('_keboola.events'));
    assert.ok(commands.includes('_keboola.tables'));
    assert.ok(commands.includes('_keboola.buckets'));
    assert.ok(commands.includes('_keboola.jobs'));
  });

  test('returns 9 rows (all commands)', () => {
    const result = helpCommand.execute('test-conn');
    assert.strictEqual(result.results.length, 9);
  });

  test('uses provided connId', () => {
    const result = helpCommand.execute('my-connection-id');
    assert.strictEqual(result.connId, 'my-connection-id');
  });

  test('result has no error flag', () => {
    const result = helpCommand.execute('test-conn');
    assert.strictEqual(result.error, undefined);
  });
});
