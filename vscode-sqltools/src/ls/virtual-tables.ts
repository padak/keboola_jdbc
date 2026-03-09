import { NSDatabase } from '@sqltools/types';
import { VIRTUAL_TABLE_DEFAULT_LIMIT } from '../constants';

/**
 * Type for the Storage API request function injected from the driver.
 * Avoids circular dependency by using dependency injection.
 */
type StorageApiRequestFn = <T = any>(
  path: string,
  options?: { method?: string; body?: string }
) => Promise<T>;

/**
 * Pattern to detect SQL queries against virtual _keboola.* tables.
 * Supports unquoted, double-quoted, and single-quoted identifiers:
 *   SELECT * FROM _keboola.components
 *   SELECT * FROM "_keboola"."events"
 *   SELECT * FROM '_keboola'.'jobs'
 */
const VIRTUAL_TABLE_PATTERN = /SELECT\s+.*\bFROM\s+["']?_keboola["']?\.["']?(\w+)["']?/i;

/**
 * Pattern to extract LIMIT N from SQL.
 */
const LIMIT_PATTERN = /LIMIT\s+(\d+)/i;

/**
 * Known virtual table names mapped to their query functions.
 */
const KNOWN_TABLES = new Set([
  'components',
  'events',
  'tables',
  'buckets',
  'jobs',
]);

/**
 * Checks whether the SQL is a query against a virtual _keboola.* table.
 */
export function canHandle(sql: string): boolean {
  const match = VIRTUAL_TABLE_PATTERN.exec(sql);
  if (!match) return false;
  return KNOWN_TABLES.has(match[1].toLowerCase());
}

/**
 * Executes a virtual table query by fetching data from the Storage API
 * and returning it in NSDatabase.IResult format.
 */
export async function execute(
  sql: string,
  connId: string,
  storageApiRequest: StorageApiRequestFn
): Promise<NSDatabase.IResult[]> {
  const tableMatch = VIRTUAL_TABLE_PATTERN.exec(sql);
  if (!tableMatch) {
    return [buildErrorResult(connId, sql, 'Could not extract virtual table name from SQL.')];
  }

  const tableName = tableMatch[1].toLowerCase();
  const limit = parseLimitFromSql(sql);

  try {
    const result = await queryTable(tableName, limit, connId, sql, storageApiRequest);
    return [result];
  } catch (err: any) {
    return [
      buildErrorResult(
        connId,
        sql,
        `Error querying _keboola.${tableName}: ${err.message}`
      ),
    ];
  }
}

/**
 * Extracts the LIMIT value from a SQL string.
 * Returns VIRTUAL_TABLE_DEFAULT_LIMIT if no LIMIT clause is found.
 */
export function parseLimitFromSql(sql: string): number {
  const match = LIMIT_PATTERN.exec(sql);
  if (match) {
    const parsed = parseInt(match[1], 10);
    if (!isNaN(parsed)) {
      return parsed;
    }
  }
  return VIRTUAL_TABLE_DEFAULT_LIMIT;
}

/**
 * Dispatches to the appropriate table query function.
 */
async function queryTable(
  tableName: string,
  limit: number,
  connId: string,
  sql: string,
  storageApiRequest: StorageApiRequestFn
): Promise<NSDatabase.IResult> {
  switch (tableName) {
    case 'components':
      return queryComponents(limit, connId, sql, storageApiRequest);
    case 'events':
      return queryEvents(limit, connId, sql, storageApiRequest);
    case 'tables':
      return queryTables(limit, connId, sql, storageApiRequest);
    case 'buckets':
      return queryBuckets(limit, connId, sql, storageApiRequest);
    case 'jobs':
      return queryJobs(limit, connId, sql, storageApiRequest);
    default:
      throw new Error(
        `Unknown virtual table: _keboola.${tableName}. Available: ${Array.from(KNOWN_TABLES).join(', ')}`
      );
  }
}

/**
 * _keboola.components: Lists components with their configurations (flattened).
 * GET /v2/storage/components
 */
async function queryComponents(
  limit: number,
  connId: string,
  sql: string,
  storageApiRequest: StorageApiRequestFn
): Promise<NSDatabase.IResult> {
  const cols = [
    'component_id',
    'component_name',
    'component_type',
    'config_id',
    'config_name',
    'config_description',
    'version',
    'created',
    'is_disabled',
  ];

  const components = await storageApiRequest<any[]>('/v2/storage/components');
  const rows: Record<string, any>[] = [];

  for (const comp of components) {
    const compId = comp.id || '';
    const compName = comp.name || '';
    const compType = comp.type || '';
    const configs = comp.configurations || [];

    for (const cfg of configs) {
      rows.push({
        component_id: compId,
        component_name: compName,
        component_type: compType,
        config_id: cfg.id || '',
        config_name: cfg.name || '',
        config_description: cfg.description || '',
        version: cfg.version || 0,
        created: cfg.created || '',
        is_disabled: cfg.isDisabled ? 'true' : 'false',
      });

      if (limit > 0 && rows.length >= limit) break;
    }
    if (limit > 0 && rows.length >= limit) break;
  }

  const limitedRows = limit > 0 ? rows.slice(0, limit) : rows;
  return buildResult(connId, sql, cols, limitedRows);
}

/**
 * _keboola.events: Lists recent storage events.
 * GET /v2/storage/events?limit=N
 */
async function queryEvents(
  limit: number,
  connId: string,
  sql: string,
  storageApiRequest: StorageApiRequestFn
): Promise<NSDatabase.IResult> {
  const cols = ['event_id', 'type', 'component', 'message', 'created'];
  const effectiveLimit = limit > 0 ? limit : VIRTUAL_TABLE_DEFAULT_LIMIT;

  const events = await storageApiRequest<any[]>(
    `/v2/storage/events?limit=${effectiveLimit}`
  );
  const rows = events.map((event: any) => ({
    event_id: event.uuid || event.id || '',
    type: event.type || '',
    component: event.component || '',
    message: event.message || '',
    created: event.created || '',
  }));

  return buildResult(connId, sql, cols, rows);
}

/**
 * _keboola.tables: Lists all tables with metadata.
 * GET /v2/storage/tables?include=columns,buckets
 */
async function queryTables(
  limit: number,
  connId: string,
  sql: string,
  storageApiRequest: StorageApiRequestFn
): Promise<NSDatabase.IResult> {
  const cols = [
    'table_id',
    'bucket_id',
    'name',
    'primary_key',
    'rows_count',
    'data_size_bytes',
    'last_import_date',
    'created',
  ];

  const tables = await storageApiRequest<any[]>(
    '/v2/storage/tables?include=columns,buckets'
  );

  let rows = tables.map((table: any) => {
    const primaryKey = Array.isArray(table.primaryKey)
      ? table.primaryKey.join(', ')
      : '';
    const bucketId = table.bucket?.id || table.bucketId || '';

    return {
      table_id: table.id || '',
      bucket_id: bucketId,
      name: table.name || '',
      primary_key: primaryKey,
      rows_count: table.rowsCount || 0,
      data_size_bytes: table.dataSizeBytes || 0,
      last_import_date: table.lastImportDate || '',
      created: table.created || '',
    };
  });

  if (limit > 0) {
    rows = rows.slice(0, limit);
  }

  return buildResult(connId, sql, cols, rows);
}

/**
 * _keboola.buckets: Lists all buckets.
 * GET /v2/storage/buckets
 */
async function queryBuckets(
  limit: number,
  connId: string,
  sql: string,
  storageApiRequest: StorageApiRequestFn
): Promise<NSDatabase.IResult> {
  const cols = [
    'bucket_id',
    'name',
    'stage',
    'description',
    'tables_count',
    'data_size_bytes',
    'created',
  ];

  const buckets = await storageApiRequest<any[]>('/v2/storage/buckets');

  let rows = buckets.map((bucket: any) => {
    const tablesCount = Array.isArray(bucket.tables)
      ? bucket.tables.length
      : 0;

    return {
      bucket_id: bucket.id || '',
      name: bucket.name || '',
      stage: bucket.stage || '',
      description: bucket.description || '',
      tables_count: tablesCount,
      data_size_bytes: bucket.dataSizeBytes || 0,
      created: bucket.created || '',
    };
  });

  if (limit > 0) {
    rows = rows.slice(0, limit);
  }

  return buildResult(connId, sql, cols, rows);
}

/**
 * _keboola.jobs: Lists recent jobs.
 * GET /v2/storage/jobs?limit=N
 */
async function queryJobs(
  limit: number,
  connId: string,
  sql: string,
  storageApiRequest: StorageApiRequestFn
): Promise<NSDatabase.IResult> {
  const cols = [
    'job_id',
    'component_id',
    'config_id',
    'status',
    'created',
    'started',
    'finished',
    'duration_sec',
  ];

  const effectiveLimit = limit > 0 ? limit : VIRTUAL_TABLE_DEFAULT_LIMIT;
  const jobs = await storageApiRequest<any[]>(
    `/v2/storage/jobs?limit=${effectiveLimit}`
  );

  const rows = jobs.map((job: any) => ({
    job_id: job.id || '',
    component_id: job.component || job.operationName || '',
    config_id: job.config || job.configId || '',
    status: job.status || '',
    created: job.createdTime || job.created || '',
    started: job.startTime || job.started || '',
    finished: job.endTime || job.finished || '',
    duration_sec: job.durationSeconds != null ? job.durationSeconds : '',
  }));

  return buildResult(connId, sql, cols, rows);
}

/**
 * Builds a successful NSDatabase.IResult from columns and row objects.
 */
function buildResult(
  connId: string,
  sql: string,
  cols: string[],
  rows: Record<string, any>[]
): NSDatabase.IResult {
  return {
    connId,
    cols,
    messages: [
      {
        date: new Date(),
        message: `Virtual table query completed: ${rows.length} rows`,
      },
    ],
    results: rows,
    query: sql,
    requestId: `virtual-${Date.now()}`,
    resultId: `virtual-${Date.now()}`,
  };
}

/**
 * Builds an error NSDatabase.IResult.
 */
function buildErrorResult(
  connId: string,
  sql: string,
  errorMessage: string
): NSDatabase.IResult {
  return {
    connId,
    cols: [],
    messages: [{ date: new Date(), message: `ERROR: ${errorMessage}` }],
    results: [],
    query: sql,
    requestId: `virtual-err-${Date.now()}`,
    resultId: `virtual-err-${Date.now()}`,
    error: true,
  };
}
