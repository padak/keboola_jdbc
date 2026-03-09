import { SCHEMA_CACHE_TTL_MS } from '../constants';

/**
 * Cached entry wrapping data and its fetch timestamp.
 */
interface CachedEntry<T> {
  data: T;
  timestamp: number;
}

/**
 * Caches Storage API metadata (buckets and tables) with a configurable TTL.
 *
 * Within the TTL window, cached data is returned without calling the fetch
 * function. After TTL expiry, the fetch function is called to refresh. If the
 * fetch fails and stale data is available, the stale data is returned as a
 * fallback. If no stale data exists (first call failure), the error propagates.
 *
 * No locking is needed because Node.js is single-threaded.
 */
export class SchemaCache {
  private readonly ttlMs: number;

  private bucketsEntry: CachedEntry<any[]> | null = null;
  private tablesEntries = new Map<string, CachedEntry<any[]>>();

  constructor(ttlMs: number = SCHEMA_CACHE_TTL_MS) {
    this.ttlMs = ttlMs;
  }

  /**
   * Returns cached buckets, refreshing via fetchFn if the cache is expired.
   * On fetch error: returns stale data if available, throws if not.
   */
  async getBuckets<T>(fetchFn: () => Promise<T[]>): Promise<T[]> {
    const now = Date.now();

    if (this.bucketsEntry && (now - this.bucketsEntry.timestamp) <= this.ttlMs) {
      return this.bucketsEntry.data as T[];
    }

    try {
      const data = await fetchFn();
      this.bucketsEntry = { data, timestamp: Date.now() };
      return data;
    } catch (err) {
      if (this.bucketsEntry) {
        // Return stale data on refresh failure
        return this.bucketsEntry.data as T[];
      }
      throw err;
    }
  }

  /**
   * Returns cached tables for a bucket, refreshing via fetchFn if the cache
   * is expired. On fetch error: returns stale data if available, throws if not.
   */
  async getTables<T>(bucketId: string, fetchFn: () => Promise<T[]>): Promise<T[]> {
    const now = Date.now();
    const entry = this.tablesEntries.get(bucketId);

    if (entry && (now - entry.timestamp) <= this.ttlMs) {
      return entry.data as T[];
    }

    try {
      const data = await fetchFn();
      this.tablesEntries.set(bucketId, { data, timestamp: Date.now() });
      return data;
    } catch (err) {
      if (entry) {
        // Return stale data on refresh failure
        return entry.data as T[];
      }
      throw err;
    }
  }

  /**
   * Invalidates all cached data (buckets and all per-bucket tables).
   */
  invalidate(): void {
    this.bucketsEntry = null;
    this.tablesEntries.clear();
  }

  /**
   * Invalidates cached tables for a specific bucket.
   */
  invalidateBucket(bucketId: string): void {
    this.tablesEntries.delete(bucketId);
  }
}
