package com.keboola.jdbc.meta;

import com.keboola.jdbc.http.StorageApiClient;
import com.keboola.jdbc.http.model.Bucket;
import com.keboola.jdbc.http.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Caches Storage API metadata (buckets and tables) with a configurable TTL.
 * Thread-safe implementation using ConcurrentHashMap.
 */
public class SchemaCache {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaCache.class);
    private static final long DEFAULT_TTL_MS = 60_000; // 60 seconds

    private final StorageApiClient storageClient;
    private final long ttlMs;

    private volatile List<Bucket> cachedBuckets;
    private volatile long bucketsTimestamp;

    private final ConcurrentHashMap<String, CachedEntry<List<TableInfo>>> tablesCache = new ConcurrentHashMap<>();

    public SchemaCache(StorageApiClient storageClient) {
        this(storageClient, DEFAULT_TTL_MS);
    }

    public SchemaCache(StorageApiClient storageClient, long ttlMs) {
        this.storageClient = storageClient;
        this.ttlMs = ttlMs;
    }

    /**
     * Returns cached buckets, refreshing if expired.
     */
    public List<Bucket> getBuckets() {
        long now = System.currentTimeMillis();
        if (cachedBuckets == null || (now - bucketsTimestamp) > ttlMs) {
            LOG.debug("Refreshing buckets cache");
            try {
                cachedBuckets = storageClient.listBuckets();
                bucketsTimestamp = now;
            } catch (Exception e) {
                LOG.warn("Failed to refresh buckets cache: {}", e.getMessage());
                if (cachedBuckets == null) {
                    return Collections.emptyList();
                }
                // Return stale data on refresh failure
            }
        }
        return cachedBuckets;
    }

    /**
     * Returns cached tables for a bucket, refreshing if expired.
     */
    public List<TableInfo> getTables(String bucketId) {
        long now = System.currentTimeMillis();
        CachedEntry<List<TableInfo>> entry = tablesCache.get(bucketId);

        if (entry == null || (now - entry.timestamp) > ttlMs) {
            LOG.debug("Refreshing tables cache for bucket: {}", bucketId);
            try {
                List<TableInfo> tables = storageClient.listTables(bucketId);
                tablesCache.put(bucketId, new CachedEntry<>(tables, now));
                return tables;
            } catch (Exception e) {
                LOG.warn("Failed to refresh tables cache for bucket {}: {}", bucketId, e.getMessage());
                if (entry == null) {
                    return Collections.emptyList();
                }
                return entry.data;
            }
        }
        return entry.data;
    }

    /**
     * Invalidates all cached data.
     */
    public void invalidate() {
        cachedBuckets = null;
        bucketsTimestamp = 0;
        tablesCache.clear();
    }

    /**
     * Invalidates cached tables for a specific bucket.
     */
    public void invalidateBucket(String bucketId) {
        tablesCache.remove(bucketId);
    }

    private static class CachedEntry<T> {
        final T data;
        final long timestamp;

        CachedEntry(T data, long timestamp) {
            this.data = data;
            this.timestamp = timestamp;
        }
    }
}
