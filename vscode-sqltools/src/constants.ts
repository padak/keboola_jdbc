import { IDriverAlias } from '@sqltools/types';

/** Driver aliases registered with SQLTools */
export const DRIVER_ALIASES: IDriverAlias[] = [
  { displayName: 'Keboola', value: 'Keboola' },
];

/**
 * Maps predefined Keboola stack connection URLs to their query service URLs.
 * Used as fallback when auto-discovery from Storage API index is unavailable.
 * Pattern: "connection.X" -> "query.X"
 */
export const KEBOOLA_STACKS: Record<string, string> = {
  'connection.keboola.com': 'query.keboola.com',
  'connection.us-east4.gcp.keboola.com': 'query.us-east4.gcp.keboola.com',
  'connection.eu-central-1.keboola.com': 'query.eu-central-1.keboola.com',
  'connection.north-europe.azure.keboola.com': 'query.north-europe.azure.keboola.com',
  'connection.europe-west3.gcp.keboola.com': 'query.europe-west3.gcp.keboola.com',
};

/**
 * Resolves the effective connection URL from driver settings.
 * Uses customConnectionUrl when stack is "custom", otherwise uses the stack value directly.
 */
export function getConnectionUrl(keboolaStack: string, customConnectionUrl?: string): string {
  if (keboolaStack === 'custom') {
    if (!customConnectionUrl) {
      throw new Error('Custom Connection URL is required when using a custom stack.');
    }
    return customConnectionUrl;
  }
  return keboolaStack;
}

/**
 * Derives the query service URL from a connection URL.
 * First checks the predefined KEBOOLA_STACKS map, then falls back to
 * replacing the "connection." prefix with "query.".
 *
 * e.g. connection.keboola.com -> query.keboola.com
 *      connection.eu-central-1.keboola.com -> query.eu-central-1.keboola.com
 */
export function getQueryUrl(connectionUrl: string): string {
  if (KEBOOLA_STACKS[connectionUrl]) {
    return KEBOOLA_STACKS[connectionUrl];
  }
  if (!connectionUrl.startsWith('connection.')) {
    throw new Error(
      `Invalid connection URL: "${connectionUrl}". ` +
      'URL must start with "connection." (e.g., connection.keboola.com).'
    );
  }
  return connectionUrl.replace(/^connection\./, 'query.');
}

/** Polling configuration for query execution with exponential backoff */
export const QUERY_POLLING = {
  INITIAL_INTERVAL_MS: 100,
  MAX_INTERVAL_MS: 2000,
  BACKOFF_MULTIPLIER: 1.5,
  DEFAULT_TIMEOUT_S: 300,
};

/** Number of result rows fetched per page from the Query Service API */
export const DEFAULT_PAGE_SIZE = 1000;

/** Maximum total result rows fetched across all pages */
export const MAX_RESULT_ROWS = 10000;

/** Schema cache time-to-live in milliseconds */
export const SCHEMA_CACHE_TTL_MS = 60000;

/** HTTP request timeout in milliseconds */
export const HTTP_TIMEOUT_MS = 30000;

/** Maximum number of retries for 5xx/429 HTTP responses */
export const MAX_RETRIES = 3;

/** Default row limit for virtual table queries when LIMIT is not specified */
export const VIRTUAL_TABLE_DEFAULT_LIMIT = 100;

/** Terminal states for query jobs (job-level) */
export const TERMINAL_STATES = new Set(['completed', 'failed', 'canceled']);

/** Terminal states for individual statements within a query job */
export const STATEMENT_TERMINAL_STATES = new Set(['completed', 'failed', 'canceled', 'notExecuted']);

/**
 * Pattern to match USE SCHEMA/DATABASE commands for local schema tracking.
 * Captures the schema/database name with or without double quotes.
 * Examples: USE SCHEMA "in.c-main", USE DATABASE my_db, use schema Foo;
 */
export const USE_PATTERN = /^\s*USE\s+(?:SCHEMA|DATABASE)\s+"?([^"\s;]+)"?\s*;?\s*$/i;
