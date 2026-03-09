import AbstractDriver from '@sqltools/base-driver';
import {
  IConnectionDriver,
  MConnectionExplorer,
  NSDatabase,
  ContextValue,
  IQueryOptions,
} from '@sqltools/types';
import queries from './queries';
import { SchemaCache } from './schema-cache';
import * as virtualTables from './virtual-tables';
import * as helpCommand from './help-command';
import {
  getConnectionUrl,
  getQueryUrl,
  QUERY_POLLING,
  TERMINAL_STATES,
  DEFAULT_PAGE_SIZE,
  MAX_RESULT_ROWS,
  MAX_RETRIES,
  HTTP_TIMEOUT_MS,
  USE_PATTERN,
} from '../constants';
import {
  KeboolaCredentials,
  QueryJobResponse,
  JobStatusResponse,
  QueryResultResponse,
  BranchResponse,
  WorkspaceResponse,
  BucketInfo,
  TableInfo,
} from '../types';
import * as crypto from 'crypto';

type DriverOptions = any;
type DriverLib = any;

/**
 * Keboola driver for SQLTools.
 *
 * Connects to Keboola Query Service for SQL execution and to the Storage API
 * for database explorer (sidebar) metadata. Supports auto-discovery of the
 * Query Service URL, default branch, and newest workspace.
 */
export default class KeboolaDriver
  extends AbstractDriver<DriverLib, DriverOptions>
  implements IConnectionDriver
{
  queries = queries;

  /** Static registry of active driver instances for cancellation support */
  private static instances = new Map<string, KeboolaDriver>();
  /** Currently executing query job ID */
  private activeQueryJobId: string | null = null;

  /** Resolved connection URL (e.g., connection.keboola.com) */
  private connectionUrl = '';
  /** Resolved query service URL (e.g., query.keboola.com) */
  private queryServiceUrl = '';
  /** Resolved branch ID */
  private branchId = '';
  /** Resolved workspace ID */
  private workspaceId = '';
  /** Session ID for server-side session persistence */
  private sessionId = '';
  /** Currently active schema set via USE SCHEMA/DATABASE */
  private currentSchema: string | null = null;
  /** Schema cache with TTL for sidebar metadata */
  private schemaCache = new SchemaCache();

  /** Typed accessor for credentials */
  private get creds(): KeboolaCredentials {
    return this.credentials as unknown as KeboolaCredentials;
  }

  // -- Connection Management --------------------------------------------------

  async open(): Promise<DriverLib> {
    if (this.connection) return this.connection;

    // 1. Resolve connection URL
    this.connectionUrl = getConnectionUrl(
      this.creds.keboolaStack,
      this.creds.customConnectionUrl
    );

    // 2. Auto-discover Query Service URL from Storage API index
    this.queryServiceUrl = await this.discoverQueryServiceUrl();

    // 3. Resolve branch ID (from config or auto-detect default)
    this.branchId = await this.resolveBranchId();

    // 4. Resolve workspace ID (from config or auto-select newest)
    this.workspaceId = await this.resolveWorkspaceId();

    // 5. Generate session ID for server-side session persistence
    this.sessionId = crypto.randomUUID();

    this.log.info(
      `Connected to Keboola: branch=${this.branchId}, workspace=${this.workspaceId}, session=${this.sessionId}`
    );

    this.connection = Promise.resolve(true as any);
    KeboolaDriver.instances.set(this.getId(), this);

    return this.connection;
  }

  async close(): Promise<void> {
    KeboolaDriver.instances.delete(this.getId());
    this.connectionUrl = '';
    this.queryServiceUrl = '';
    this.branchId = '';
    this.workspaceId = '';
    this.sessionId = '';
    this.currentSchema = null;
    this.schemaCache.invalidate();
    this.connection = undefined as any;
  }

  async testConnection(): Promise<void> {
    await this.open();
    await this.query('SELECT 1');
  }

  // -- Auto-discovery ---------------------------------------------------------

  /**
   * Discovers the Query Service URL from the Storage API index.
   * Falls back to the deterministic URL derivation if auto-discovery fails.
   */
  private async discoverQueryServiceUrl(): Promise<string> {
    try {
      const index = await this.storageApiRequest<{
        services: { id: string; url: string }[];
      }>('/v2/storage');
      const querySvc = index.services?.find((s) => s.id === 'query');
      if (querySvc?.url) {
        // Extract hostname from full URL (e.g., "https://query.keboola.com" -> "query.keboola.com")
        const url = new URL(querySvc.url);
        return url.hostname;
      }
    } catch (err: any) {
      this.log.warn(`Auto-discovery of Query Service URL failed: ${err.message}. Using fallback.`);
    }

    // Fallback: derive from connection URL using naming convention
    return getQueryUrl(this.connectionUrl);
  }

  /**
   * Resolves the branch ID. Uses the configured value if provided,
   * otherwise auto-detects the default branch from the Storage API.
   */
  private async resolveBranchId(): Promise<string> {
    if (this.creds.branchId) {
      return String(this.creds.branchId);
    }

    const branches = await this.storageApiRequest<BranchResponse[]>(
      '/v2/storage/dev-branches'
    );
    const defaultBranch = branches.find((b) => b.isDefault);
    if (!defaultBranch) {
      throw new Error('No default branch found in the project.');
    }

    this.log.info(`Auto-detected default branch: id=${defaultBranch.id}, name="${defaultBranch.name}"`);
    return String(defaultBranch.id);
  }

  /**
   * Resolves the workspace ID. Uses the configured value if provided,
   * otherwise auto-selects the newest (last) workspace.
   */
  private async resolveWorkspaceId(): Promise<string> {
    if (this.creds.workspaceId) {
      return String(this.creds.workspaceId);
    }

    const workspaces = await this.storageApiRequest<WorkspaceResponse[]>(
      '/v2/storage/workspaces'
    );
    if (workspaces.length === 0) {
      throw new Error(
        'No workspaces found in the project. Create a workspace in Keboola UI first, then reconnect.'
      );
    }

    const newest = workspaces[workspaces.length - 1];
    this.log.info(
      `Auto-selected workspace: id=${newest.id}, name="${newest.name}" (${workspaces.length} available)`
    );
    return String(newest.id);
  }

  // -- Query Execution --------------------------------------------------------

  query(queryOrQueries: string, _opt?: IQueryOptions): Promise<NSDatabase.IResult[]>;
  query(
    queryOrQueries: string | string[],
    _opt?: IQueryOptions
  ): Promise<NSDatabase.IResult[]> {
    const queries = Array.isArray(queryOrQueries) ? queryOrQueries : [queryOrQueries];

    return this.open()
      .then(async () => {
        const results: NSDatabase.IResult[] = [];
        for (const sql of queries) {
          // 1. Check KEBOOLA HELP command (before any API calls)
          if (helpCommand.canHandle(sql)) {
            results.push(helpCommand.execute(this.getId()));
            continue;
          }

          // 2. Check virtual table queries (before Query Service)
          if (virtualTables.canHandle(sql)) {
            results.push(
              ...(await virtualTables.execute(
                sql,
                this.getId(),
                this.storageApiRequest.bind(this)
              ))
            );
            continue;
          }

          // 3. Check SHOW HISTORY command
          const trimmed = sql.trim().replace(/;\s*$/, '').trim().toUpperCase();
          if (trimmed === 'SHOW HISTORY' || trimmed === 'SHOW QUERY HISTORY') {
            results.push(...(await this.fetchWorkspaceQueryHistory(sql)));
          } else {
            results.push(...(await this.executeQuery(sql)));
          }
        }
        return results;
      })
      .catch((err) => [this.buildErrorResult(queries[0] || '', err.message)]);
  }

  private async executeQuery(sql: string): Promise<NSDatabase.IResult[]> {
    // Check if this is a USE SCHEMA/DATABASE command
    const useMatch = sql.match(USE_PATTERN);
    const isUseCommand = useMatch !== null;

    if (isUseCommand) {
      // Update local schema tracking
      this.currentSchema = useMatch[1];
      this.log.info(`Schema set to: ${this.currentSchema}`);
    }

    // Build statements array:
    // - If currentSchema is set AND this is NOT a USE command, prepend USE SCHEMA
    //   so both run in the same Snowflake session
    // - Otherwise, just send the original SQL
    const statements: string[] =
      this.currentSchema && !isUseCommand
        ? [`USE SCHEMA "${this.currentSchema}"`, sql]
        : [sql];

    // 1. Submit the query job
    const submitResponse = await this.queryApiRequest<QueryJobResponse>(
      `/api/v1/branches/${this.branchId}/workspaces/${this.workspaceId}/queries`,
      {
        method: 'POST',
        body: JSON.stringify({
          statements,
          sessionId: this.sessionId,
          wait: false,
        }),
      }
    );

    const queryJobId = submitResponse?.queryJobId;
    if (!queryJobId) {
      return [this.buildErrorResult(sql, 'Unexpected API response: missing queryJobId.')];
    }
    this.activeQueryJobId = queryJobId;

    try {
      // 2. Poll until terminal state
      const jobResult = await this.pollQueryJob(queryJobId);

      if (jobResult.status === 'failed') {
        // For multi-statement jobs, check the last statement for errors (the actual query)
        const jobStatements = jobResult.statements || [];
        const lastStmt = jobStatements[jobStatements.length - 1];
        const errorMsg =
          lastStmt?.error?.message ||
          jobStatements[0]?.error?.message ||
          'Query execution failed';
        return [this.buildErrorResult(sql, errorMsg)];
      }

      if (jobResult.status === 'canceled') {
        return [this.buildErrorResult(sql, 'Query was canceled.')];
      }

      // 3. For multi-statement jobs (with USE SCHEMA prepended), use only the LAST
      //    statement's results (the actual user query), not the USE SCHEMA results.
      const jobStatements = jobResult.statements || [];
      const results: NSDatabase.IResult[] = [];

      if (statements.length > 1 && jobStatements.length > 1) {
        // Multi-statement: only process the last statement (the actual query)
        const lastStmt = jobStatements[jobStatements.length - 1];
        if (lastStmt.status === 'completed') {
          const resultData = await this.fetchAllPages(queryJobId, lastStmt.id);
          results.push(this.mapResultToSQLTools(sql, resultData));
        } else if (lastStmt.status === 'failed') {
          results.push(
            this.buildErrorResult(sql, lastStmt.error?.message || 'Statement execution failed')
          );
        } else {
          results.push(this.buildErrorResult(sql, `Statement status: ${lastStmt.status}`));
        }
      } else {
        // Single statement: process all results as before
        for (const stmt of jobStatements) {
          if (stmt.status === 'completed') {
            const resultData = await this.fetchAllPages(queryJobId, stmt.id);
            results.push(this.mapResultToSQLTools(sql, resultData));
          } else if (stmt.status === 'failed') {
            results.push(
              this.buildErrorResult(sql, stmt.error?.message || 'Statement execution failed')
            );
          } else {
            results.push(this.buildErrorResult(sql, `Statement status: ${stmt.status}`));
          }
        }
      }

      // If no statements returned, provide a default success result
      if (results.length === 0) {
        results.push({
          connId: this.getId(),
          cols: [],
          messages: [{ date: new Date(), message: 'Query completed successfully.' }],
          results: [],
          query: sql,
          requestId: queryJobId,
          resultId: `${queryJobId}-0`,
        });
      }

      return results;
    } finally {
      this.activeQueryJobId = null;
    }
  }

  // -- Query History ----------------------------------------------------------

  private async fetchWorkspaceQueryHistory(originalSql: string): Promise<NSDatabase.IResult[]> {
    const history = await this.queryApiRequest<any>(
      `/api/v1/branches/${this.branchId}/workspaces/${this.workspaceId}/queries?pageSize=500`
    );

    const cols = ['Job ID', 'Status', 'Query', 'Created At', 'Completed At'];
    const items = Array.isArray(history) ? history : history.statements || [];
    const rows = items.map((item: any) => ({
      'Job ID': item.queryJobId || '',
      'Status': item.status || '',
      'Query': (item.statements || []).map((s: any) => s.sql || '').join('; '),
      'Created At': item.createdAt || '',
      'Completed At': item.completedAt || '',
    }));

    return [
      {
        connId: this.getId(),
        cols,
        messages: [{ date: new Date(), message: `${rows.length} history entries returned` }],
        results: rows,
        query: originalSql,
        requestId: `history-${Date.now()}`,
        resultId: `history-${Date.now()}`,
      },
    ];
  }

  // -- Polling ----------------------------------------------------------------

  private async pollQueryJob(queryJobId: string): Promise<JobStatusResponse> {
    let interval = QUERY_POLLING.INITIAL_INTERVAL_MS;
    const timeoutMs = QUERY_POLLING.DEFAULT_TIMEOUT_S * 1000;
    const startTime = Date.now();

    while (true) {
      const job = await this.queryApiRequest<JobStatusResponse>(
        `/api/v1/queries/${queryJobId}`
      );

      if (TERMINAL_STATES.has(job.status)) {
        return job;
      }

      if (Date.now() - startTime > timeoutMs) {
        // Attempt to cancel the timed-out query
        await this.cancelQuery(queryJobId).catch(() => {});
        throw new Error(
          `Query timed out after ${QUERY_POLLING.DEFAULT_TIMEOUT_S} seconds.`
        );
      }

      await this.sleep(interval);
      interval = Math.min(
        interval * QUERY_POLLING.BACKOFF_MULTIPLIER,
        QUERY_POLLING.MAX_INTERVAL_MS
      );
    }
  }

  // -- Pagination -------------------------------------------------------------

  /**
   * Fetches all result pages for a completed statement, up to MAX_RESULT_ROWS.
   */
  private async fetchAllPages(
    queryJobId: string,
    statementId: string
  ): Promise<QueryResultResponse> {
    let page = 0;
    let allData: any[][] = [];
    let columns: { name: string; type?: string }[] = [];
    let totalRows = 0;

    while (true) {
      const offset = page * DEFAULT_PAGE_SIZE;
      const result = await this.queryApiRequest<QueryResultResponse>(
        `/api/v1/queries/${queryJobId}/${statementId}/results?offset=${offset}&pageSize=${DEFAULT_PAGE_SIZE}`
      );

      if (page === 0) {
        columns = result.columns || [];
        totalRows = result.numberOfRows ?? 0;
      }

      const pageData = result.data || [];
      allData = allData.concat(pageData);

      // Stop if we have all rows or hit the limit
      if (
        pageData.length < DEFAULT_PAGE_SIZE ||
        allData.length >= totalRows ||
        allData.length >= MAX_RESULT_ROWS
      ) {
        break;
      }

      page++;
    }

    // Trim to MAX_RESULT_ROWS if we exceeded
    if (allData.length > MAX_RESULT_ROWS) {
      allData = allData.slice(0, MAX_RESULT_ROWS);
    }

    return {
      columns,
      data: allData,
      numberOfRows: totalRows,
    };
  }

  // -- Cancellation -----------------------------------------------------------

  private async cancelQuery(queryJobId: string): Promise<void> {
    await this.queryApiRequest(`/api/v1/queries/${queryJobId}/cancel`, {
      method: 'POST',
      body: JSON.stringify({ reason: 'Cancelled by user from VS Code' }),
    });
  }

  /** Cancel all active queries across all driver instances. Called via LS request. */
  static cancelAllActiveQueries(): { cancelled: string[] } {
    const cancelled: string[] = [];
    for (const [, driver] of KeboolaDriver.instances) {
      if (driver.activeQueryJobId) {
        const jobId = driver.activeQueryJobId;
        driver.cancelQuery(jobId).catch(() => {});
        cancelled.push(jobId);
      }
    }
    return { cancelled };
  }

  // -- Result Mapping ---------------------------------------------------------

  private mapResultToSQLTools(sql: string, resultData: QueryResultResponse): NSDatabase.IResult {
    const columns: string[] = (resultData.columns || []).map((c) => c.name);
    const rows: any[] = (resultData.data || []).map((row: any[]) => {
      const obj: Record<string, any> = {};
      columns.forEach((col, i) => {
        obj[col] = row[i];
      });
      return obj;
    });

    return {
      connId: this.getId(),
      cols: columns,
      messages: [
        {
          date: new Date(),
          message: `Query completed: ${rows.length} rows` +
            (resultData.numberOfRows && resultData.numberOfRows > rows.length
              ? ` (${resultData.numberOfRows} total, limited to ${MAX_RESULT_ROWS})`
              : ''),
        },
      ],
      results: rows,
      query: sql,
      pageSize: rows.length,
    };
  }

  private buildErrorResult(sql: string, errorMessage: string): NSDatabase.IResult {
    return {
      connId: this.getId(),
      cols: [],
      messages: [{ date: new Date(), message: `ERROR: ${errorMessage}` }],
      results: [],
      query: sql,
      requestId: `req-${Date.now()}`,
      resultId: `res-${Date.now()}`,
      error: true,
    };
  }

  // -- Database Explorer (Sidebar) --------------------------------------------

  async getChildrenForItem(params: {
    item: NSDatabase.SearchableItem;
    parent?: NSDatabase.SearchableItem;
  }): Promise<MConnectionExplorer.IChildItem[]> {
    await this.open();
    const { item } = params;

    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return this.getBuckets();

      case ContextValue.SCHEMA:
        return [
          {
            label: 'Tables',
            type: ContextValue.RESOURCE_GROUP,
            schema: item.label,
            database: 'keboola',
            iconId: 'folder',
            childType: ContextValue.TABLE,
          },
        ];

      case ContextValue.RESOURCE_GROUP:
        return this.getTablesForBucket(item.schema);

      case ContextValue.TABLE:
        return this.getColumnsForTable(item.schema, item.label);

      default:
        return [];
    }
  }

  private async getBuckets(): Promise<MConnectionExplorer.IChildItem[]> {
    const buckets = await this.schemaCache.getBuckets<BucketInfo>(
      () => this.storageApiRequest<BucketInfo[]>('/v2/storage/buckets')
    );
    return buckets.map((bucket) => ({
      label: bucket.id,
      type: ContextValue.SCHEMA,
      schema: bucket.id,
      database: 'keboola',
      iconId: 'group-by-ref-type',
      detail: bucket.description || undefined,
    }));
  }

  private async getTablesForBucket(
    bucketId: string
  ): Promise<MConnectionExplorer.IChildItem[]> {
    const tables = await this.schemaCache.getTables<TableInfo>(
      bucketId,
      () => this.storageApiRequest<TableInfo[]>(
        `/v2/storage/buckets/${encodeURIComponent(bucketId)}/tables`
      )
    );
    return tables.map((table) => ({
      label: table.name,
      type: ContextValue.TABLE,
      schema: bucketId,
      database: 'keboola',
      isView: false,
    }));
  }

  private async getColumnsForTable(
    bucketId: string,
    tableName: string
  ): Promise<MConnectionExplorer.IChildItem[]> {
    const tableId = `${bucketId}.${tableName}`;
    const tableDetail = await this.storageApiRequest<any>(
      `/v2/storage/tables/${encodeURIComponent(tableId)}`
    );

    // Build a lookup from the typed table definition (preferred source for datatypes)
    const definitionColumns: any[] = tableDetail.definition?.columns || [];
    const definitionMap = new Map<
      string,
      { type: string; basetype: string; nullable: boolean }
    >();
    for (const defCol of definitionColumns) {
      definitionMap.set(defCol.name, {
        type: defCol.definition?.type || '',
        basetype: defCol.basetype || '',
        nullable: defCol.definition?.nullable !== false,
      });
    }

    // Fallback: columnMetadata (older/extractor-provided types)
    const columnMetadata: Record<string, any[]> = tableDetail.columnMetadata || {};
    const columns: string[] = tableDetail.columns || [];

    return columns.map((colName) => {
      const def = definitionMap.get(colName);
      let dataType = 'STRING';
      let isNullable = true;

      if (def?.type) {
        dataType = def.type;
        isNullable = def.nullable;
      } else if (def?.basetype) {
        dataType = def.basetype;
        isNullable = def.nullable;
      } else {
        const meta = columnMetadata[colName] || [];
        const typeMeta = meta.find((m: any) => m.key === 'KBC.datatype.type');
        if (typeMeta?.value) {
          dataType = typeMeta.value;
        }
      }

      return {
        label: colName,
        type: ContextValue.COLUMN,
        schema: bucketId,
        database: 'keboola',
        dataType,
        isNullable,
        table: tableName,
        detail: dataType,
      };
    });
  }

  // -- Search -----------------------------------------------------------------

  async searchItems(
    itemType: ContextValue,
    search: string,
    _extraParams?: any
  ): Promise<NSDatabase.SearchableItem[]> {
    await this.open();
    const searchLower = search.toLowerCase();

    if (itemType === ContextValue.TABLE) {
      const buckets = await this.storageApiRequest<BucketInfo[]>('/v2/storage/buckets');
      const results: NSDatabase.SearchableItem[] = [];

      for (const bucket of buckets) {
        const tables = await this.storageApiRequest<TableInfo[]>(
          `/v2/storage/buckets/${encodeURIComponent(bucket.id)}/tables`
        );
        for (const table of tables) {
          if (table.name.toLowerCase().includes(searchLower)) {
            results.push({
              label: table.name,
              type: ContextValue.TABLE,
              schema: bucket.id,
              database: 'keboola',
              isView: false,
            } as NSDatabase.SearchableItem);
          }
        }
      }
      return results;
    }

    return [];
  }

  // -- Static Completions -----------------------------------------------------

  getStaticCompletions(): Promise<{ [word: string]: NSDatabase.IStaticCompletion }> {
    const keywords = [
      'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'EXISTS',
      'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
      'CREATE', 'ALTER', 'DROP', 'TABLE', 'VIEW', 'INDEX', 'SCHEMA',
      'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'CROSS', 'ON',
      'GROUP', 'BY', 'ORDER', 'ASC', 'DESC', 'HAVING',
      'LIMIT', 'OFFSET', 'FETCH', 'FIRST', 'NEXT', 'ROWS', 'ONLY',
      'UNION', 'ALL', 'INTERSECT', 'EXCEPT', 'MINUS',
      'AS', 'DISTINCT', 'BETWEEN', 'LIKE', 'ILIKE', 'IS', 'NULL',
      'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
      'CAST', 'COALESCE', 'NULLIF',
      'COUNT', 'SUM', 'AVG', 'MIN', 'MAX',
      'WITH', 'RECURSIVE',
      'TRUE', 'FALSE',
      'BEGIN', 'COMMIT', 'ROLLBACK',
      'USE',
      'SHOW HISTORY',
    ];

    const completions: { [word: string]: NSDatabase.IStaticCompletion } = {};
    for (const kw of keywords) {
      completions[kw] = {
        label: kw,
        detail: kw === 'SHOW HISTORY'
          ? 'Keboola: Show workspace query history'
          : 'SQL Keyword',
        filterText: kw,
        sortText: kw === 'SHOW HISTORY' ? '0000' : `9999${kw}`,
        documentation: {
          kind: 'markdown',
          value: kw === 'SHOW HISTORY'
            ? 'Fetches query history from the Keboola workspace via the Query Service API.'
            : `SQL keyword: \`${kw}\``,
        },
      };
    }
    return Promise.resolve(completions);
  }

  // -- HTTP Helpers -----------------------------------------------------------

  /** Make an authenticated request to the Keboola Storage API */
  async storageApiRequest<T = any>(
    path: string,
    options?: { method?: string; body?: string }
  ): Promise<T> {
    return this.apiRequest<T>(this.connectionUrl, path, options);
  }

  /** Make an authenticated request to the Keboola Query Service API */
  private async queryApiRequest<T = any>(
    path: string,
    options?: { method?: string; body?: string }
  ): Promise<T> {
    return this.apiRequest<T>(this.queryServiceUrl, path, options);
  }

  /** Core HTTP helper with authentication, retry, and error mapping */
  private async apiRequest<T = any>(
    baseUrl: string,
    path: string,
    options?: { method?: string; body?: string }
  ): Promise<T> {
    const url = `https://${baseUrl}${path}`;
    const method = options?.method || 'GET';

    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), HTTP_TIMEOUT_MS);

        let response: Response;
        try {
          response = await fetch(url, {
            method,
            headers: {
              'X-StorageApi-Token': this.creds.token,
              'Content-Type': 'application/json',
            },
            body: options?.body,
            signal: controller.signal,
          });
        } finally {
          clearTimeout(timeoutId);
        }

        if (response.ok) {
          const text = await response.text();
          if (!text) return {} as T;
          try {
            return JSON.parse(text) as T;
          } catch {
            throw new Error(`Invalid JSON response from ${baseUrl}${path}`);
          }
        }

        // Non-retryable errors
        if (response.status === 401) {
          throw new Error(
            'Invalid or expired Storage API token. Please check your connection settings.'
          );
        }
        if (response.status === 403) {
          const body = await response.json().catch(() => ({}));
          throw new Error(
            `Access denied: ${body.message || body.exception || 'Insufficient permissions.'}`
          );
        }
        if (response.status === 404) {
          throw new Error(
            'Resource not found. Verify your Branch ID and Workspace ID.'
          );
        }
        if (response.status === 400) {
          const body = await response.json().catch(() => ({}));
          throw new Error(
            body.message || body.exception || 'Bad request. Please check your query or parameters.'
          );
        }

        // Retryable errors: 5xx and 429
        const retryable = response.status === 429 || response.status >= 500;
        if (!retryable || attempt >= MAX_RETRIES) {
          if (response.status >= 500) {
            throw new Error('Keboola service error. Please try again later.');
          }
          const body = await response.json().catch(() => ({}));
          throw new Error(
            body.message || body.exception || `Unexpected API error (HTTP ${response.status}).`
          );
        }

        lastError = new Error(`HTTP ${response.status} from ${url}`);
      } catch (err: any) {
        // If it's a mapped error (not a network error), rethrow immediately
        if (
          err.message?.includes('Invalid or expired') ||
          err.message?.includes('Access denied') ||
          err.message?.includes('Resource not found') ||
          err.message?.includes('Bad request') ||
          err.message?.includes('Keboola service error') ||
          err.message?.includes('Invalid JSON')
        ) {
          throw err;
        }

        // Network/abort errors are retryable
        if (attempt >= MAX_RETRIES) {
          const cause = err.cause;
          let detail = err.message || 'unknown error';
          if (cause) {
            detail = cause.code
              ? `${cause.code}: ${cause.message}`
              : cause.message || detail;
          }
          throw new Error(`Network error connecting to ${baseUrl}: ${detail}`);
        }

        lastError = err;
      }

      // Exponential backoff before retry
      const delay = QUERY_POLLING.INITIAL_INTERVAL_MS * Math.pow(QUERY_POLLING.BACKOFF_MULTIPLIER, attempt - 1);
      await this.sleep(delay);
    }

    throw lastError || new Error(`Request failed after ${MAX_RETRIES} attempts`);
  }

  // -- Utilities --------------------------------------------------------------

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
