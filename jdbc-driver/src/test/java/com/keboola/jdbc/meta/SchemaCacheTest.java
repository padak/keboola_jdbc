package com.keboola.jdbc.meta;

import com.keboola.jdbc.http.StorageApiClient;
import com.keboola.jdbc.http.model.Bucket;
import com.keboola.jdbc.http.model.TableInfo;
import com.keboola.jdbc.exception.KeboolaJdbcException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SchemaCache - verifies caching behaviour, TTL expiry, and cache invalidation.
 */
@ExtendWith(MockitoExtension.class)
class SchemaCacheTest {

    @Mock
    private StorageApiClient mockApiClient;

    private SchemaCache schemaCache;

    // A very short TTL (1 ms) used in tests that need to simulate TTL expiry
    private static final long SHORT_TTL_MS = 1L;
    // A very long TTL (10 minutes) used in tests that need a valid cache
    private static final long LONG_TTL_MS = 600_000L;

    @BeforeEach
    void setUp() {
        // Default cache with long TTL - individual tests can create their own with short TTL
        schemaCache = new SchemaCache(mockApiClient, LONG_TTL_MS);
    }

    // -------------------------------------------------------------------------
    // getBuckets() - API call behaviour
    // -------------------------------------------------------------------------

    @Test
    void getBuckets_firstCall_callsApi() throws KeboolaJdbcException {
        List<Bucket> expected = Arrays.asList(
                new Bucket("in.c-main", "main", "in", "", "snowflake"),
                new Bucket("out.c-reports", "reports", "out", "", "snowflake")
        );
        when(mockApiClient.listBuckets()).thenReturn(expected);

        List<Bucket> result = schemaCache.getBuckets();

        assertEquals(2, result.size());
        verify(mockApiClient, times(1)).listBuckets();
    }

    @Test
    void getBuckets_secondCallWithinTtl_usesCacheAndDoesNotCallApiAgain() throws KeboolaJdbcException {
        List<Bucket> expected = Collections.singletonList(
                new Bucket("in.c-main", "main", "in", "", "snowflake")
        );
        when(mockApiClient.listBuckets()).thenReturn(expected);

        // First call - populates cache
        schemaCache.getBuckets();
        // Second call - should use cache
        List<Bucket> result = schemaCache.getBuckets();

        assertEquals(1, result.size());
        // API should only be called once despite two getBuckets() invocations
        verify(mockApiClient, times(1)).listBuckets();
    }

    @Test
    void getBuckets_afterTtlExpiry_refreshesCacheByCallingApiAgain() throws KeboolaJdbcException, InterruptedException {
        SchemaCache shortTtlCache = new SchemaCache(mockApiClient, SHORT_TTL_MS);

        List<Bucket> firstBatch = Collections.singletonList(
                new Bucket("in.c-first", "first", "in", "", "snowflake")
        );
        List<Bucket> secondBatch = Arrays.asList(
                new Bucket("in.c-first", "first", "in", "", "snowflake"),
                new Bucket("in.c-second", "second", "in", "", "snowflake")
        );

        when(mockApiClient.listBuckets())
                .thenReturn(firstBatch)
                .thenReturn(secondBatch);

        // First call
        List<Bucket> first = shortTtlCache.getBuckets();
        assertEquals(1, first.size());

        // Wait for TTL to expire
        Thread.sleep(SHORT_TTL_MS + 5);

        // Second call should refresh
        List<Bucket> second = shortTtlCache.getBuckets();
        assertEquals(2, second.size());

        verify(mockApiClient, times(2)).listBuckets();
    }

    @Test
    void getBuckets_returnsEmptyList_whenApiReturnsEmptyList() throws KeboolaJdbcException {
        when(mockApiClient.listBuckets()).thenReturn(Collections.emptyList());

        List<Bucket> result = schemaCache.getBuckets();

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void getBuckets_whenApiThrowsAndNoCachedData_returnsEmptyList() throws KeboolaJdbcException {
        when(mockApiClient.listBuckets()).thenThrow(new KeboolaJdbcException("Network error"));

        List<Bucket> result = schemaCache.getBuckets();

        assertNotNull(result);
        assertTrue(result.isEmpty(), "Should return empty list when API fails and no cached data exists");
    }

    @Test
    void getBuckets_whenApiThrowsButCachedDataExists_returnsStaleCachedData() throws KeboolaJdbcException, InterruptedException {
        SchemaCache shortTtlCache = new SchemaCache(mockApiClient, SHORT_TTL_MS);

        List<Bucket> staleData = Collections.singletonList(
                new Bucket("in.c-stale", "stale", "in", "", "snowflake")
        );

        when(mockApiClient.listBuckets())
                .thenReturn(staleData)
                .thenThrow(new KeboolaJdbcException("API unavailable"));

        // Populate cache
        shortTtlCache.getBuckets();

        // Wait for TTL to expire
        Thread.sleep(SHORT_TTL_MS + 5);

        // Second call - API fails but stale data should be returned
        List<Bucket> result = shortTtlCache.getBuckets();

        assertEquals(1, result.size());
        assertEquals("in.c-stale", result.get(0).getId());
    }

    // -------------------------------------------------------------------------
    // getTables() - API call behaviour
    // -------------------------------------------------------------------------

    @Test
    void getTables_firstCall_callsApiWithCorrectBucketId() throws KeboolaJdbcException {
        String bucketId = "in.c-main";
        List<TableInfo> tables = Collections.singletonList(
                new TableInfo("in.c-main.orders", "orders", null, Collections.emptyList(), Collections.emptyMap())
        );
        when(mockApiClient.listTables(bucketId)).thenReturn(tables);

        List<TableInfo> result = schemaCache.getTables(bucketId);

        assertEquals(1, result.size());
        assertEquals("in.c-main.orders", result.get(0).getId());
        verify(mockApiClient, times(1)).listTables(bucketId);
    }

    @Test
    void getTables_secondCallWithinTtl_usesCacheForSameBucket() throws KeboolaJdbcException {
        String bucketId = "in.c-main";
        List<TableInfo> tables = Collections.singletonList(
                new TableInfo("in.c-main.orders", "orders", null, Collections.emptyList(), Collections.emptyMap())
        );
        when(mockApiClient.listTables(bucketId)).thenReturn(tables);

        // Populate cache
        schemaCache.getTables(bucketId);
        // Should use cache
        List<TableInfo> result = schemaCache.getTables(bucketId);

        assertEquals(1, result.size());
        verify(mockApiClient, times(1)).listTables(bucketId);
    }

    @Test
    void getTables_differentBuckets_callsApiForEach() throws KeboolaJdbcException {
        String bucket1 = "in.c-main";
        String bucket2 = "out.c-reports";

        when(mockApiClient.listTables(bucket1)).thenReturn(Collections.emptyList());
        when(mockApiClient.listTables(bucket2)).thenReturn(Collections.emptyList());

        schemaCache.getTables(bucket1);
        schemaCache.getTables(bucket2);

        // Each bucket must be fetched independently
        verify(mockApiClient, times(1)).listTables(bucket1);
        verify(mockApiClient, times(1)).listTables(bucket2);
    }

    @Test
    void getTables_afterTtlExpiry_refreshesCacheForBucket() throws KeboolaJdbcException, InterruptedException {
        SchemaCache shortTtlCache = new SchemaCache(mockApiClient, SHORT_TTL_MS);
        String bucketId = "in.c-main";

        when(mockApiClient.listTables(bucketId)).thenReturn(Collections.emptyList());

        shortTtlCache.getTables(bucketId);

        Thread.sleep(SHORT_TTL_MS + 5);

        shortTtlCache.getTables(bucketId);

        verify(mockApiClient, times(2)).listTables(bucketId);
    }

    // -------------------------------------------------------------------------
    // invalidate()
    // -------------------------------------------------------------------------

    @Test
    void invalidate_clearsAllCaches_causesApiCallOnNextAccess() throws KeboolaJdbcException {
        String bucketId = "in.c-main";

        when(mockApiClient.listBuckets()).thenReturn(Collections.emptyList());
        when(mockApiClient.listTables(bucketId)).thenReturn(Collections.emptyList());

        // Populate both caches
        schemaCache.getBuckets();
        schemaCache.getTables(bucketId);

        // Invalidate everything
        schemaCache.invalidate();

        // Access again - should call API
        schemaCache.getBuckets();
        schemaCache.getTables(bucketId);

        verify(mockApiClient, times(2)).listBuckets();
        verify(mockApiClient, times(2)).listTables(bucketId);
    }

    @Test
    void invalidate_calledMultipleTimes_doesNotThrow() throws KeboolaJdbcException {
        when(mockApiClient.listBuckets()).thenReturn(Collections.emptyList());
        schemaCache.getBuckets();

        assertDoesNotThrow(() -> {
            schemaCache.invalidate();
            schemaCache.invalidate();
        });
    }

    @Test
    void invalidate_afterCachingBuckets_nextGetBucketsCallsApi() throws KeboolaJdbcException {
        when(mockApiClient.listBuckets()).thenReturn(Collections.emptyList());

        schemaCache.getBuckets(); // First call - populates cache
        schemaCache.invalidate(); // Clear cache
        schemaCache.getBuckets(); // Should call API again

        verify(mockApiClient, times(2)).listBuckets();
    }
}
