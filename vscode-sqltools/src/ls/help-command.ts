import { NSDatabase } from '@sqltools/types';

/**
 * Pattern to match the KEBOOLA HELP command.
 * Case-insensitive, optional trailing semicolon.
 */
const HELP_PATTERN = /^\s*KEBOOLA\s+HELP\s*;?\s*$/i;

/**
 * Checks whether the SQL is a KEBOOLA HELP command.
 */
export function canHandle(sql: string): boolean {
  return HELP_PATTERN.test(sql);
}

/**
 * Executes the KEBOOLA HELP command and returns a table listing
 * all available Keboola-specific commands and virtual tables.
 */
export function execute(connId: string): NSDatabase.IResult {
  const cols = ['command', 'syntax', 'description'];

  const rows = [
    {
      command: 'KEBOOLA HELP',
      syntax: 'KEBOOLA HELP',
      description: 'Show this help message',
    },
    {
      command: 'SHOW HISTORY',
      syntax: 'SHOW HISTORY / SHOW QUERY HISTORY',
      description: 'Show workspace query history',
    },
    {
      command: 'USE SCHEMA',
      syntax: 'USE SCHEMA "schema_name"',
      description: 'Set default schema for queries',
    },
    {
      command: 'USE DATABASE',
      syntax: 'USE DATABASE "db_name"',
      description: 'Set default database',
    },
    {
      command: '_keboola.components',
      syntax: 'SELECT * FROM _keboola.components [LIMIT N]',
      description: 'List components and configurations',
    },
    {
      command: '_keboola.events',
      syntax: 'SELECT * FROM _keboola.events [LIMIT N]',
      description: 'List recent storage events',
    },
    {
      command: '_keboola.tables',
      syntax: 'SELECT * FROM _keboola.tables [LIMIT N]',
      description: 'List all tables with metadata',
    },
    {
      command: '_keboola.buckets',
      syntax: 'SELECT * FROM _keboola.buckets [LIMIT N]',
      description: 'List all buckets',
    },
    {
      command: '_keboola.jobs',
      syntax: 'SELECT * FROM _keboola.jobs [LIMIT N]',
      description: 'List recent jobs',
    },
  ];

  return {
    connId,
    cols,
    messages: [
      {
        date: new Date(),
        message: `${rows.length} commands available. Use virtual tables with SELECT * FROM _keboola.<table> [LIMIT N]`,
      },
    ],
    results: rows,
    query: 'KEBOOLA HELP',
    requestId: `help-${Date.now()}`,
    resultId: `help-${Date.now()}`,
  };
}
