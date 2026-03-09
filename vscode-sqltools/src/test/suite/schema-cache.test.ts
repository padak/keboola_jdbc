import * as assert from 'assert';
import { SchemaCache } from '../../ls/schema-cache';

suite('SchemaCache - Buckets', () => {
  test('cache hit within TTL: fetchFn is not called a second time', async () => {
    const cache = new SchemaCache(1000);
    let callCount = 0;
    const fetchFn = async () => {
      callCount++;
      return [{ id: 'in.c-main' }, { id: 'out.c-results' }];
    };

    const first = await cache.getBuckets(fetchFn);
    const second = await cache.getBuckets(fetchFn);

    assert.strictEqual(callCount, 1, 'fetchFn should be called only once within TTL');
    assert.deepStrictEqual(first, second);
    assert.strictEqual(first.length, 2);
  });

  test('cache miss after TTL expiry: fetchFn is called again', async () => {
    const cache = new SchemaCache(10); // 10ms TTL
    let callCount = 0;
    const fetchFn = async () => {
      callCount++;
      return [{ id: `bucket-${callCount}` }];
    };

    await cache.getBuckets(fetchFn);
    assert.strictEqual(callCount, 1);

    // Wait for TTL to expire
    await new Promise((resolve) => setTimeout(resolve, 20));

    const result = await cache.getBuckets(fetchFn);
    assert.strictEqual(callCount, 2, 'fetchFn should be called again after TTL expiry');
    assert.deepStrictEqual(result, [{ id: 'bucket-2' }]);
  });

  test('stale-on-error: returns old cached data when fetch fails', async () => {
    const cache = new SchemaCache(10); // 10ms TTL
    const originalData = [{ id: 'in.c-main' }];
    let shouldFail = false;

    const fetchFn = async () => {
      if (shouldFail) {
        throw new Error('API unavailable');
      }
      return originalData;
    };

    // First call succeeds and populates cache
    const first = await cache.getBuckets(fetchFn);
    assert.deepStrictEqual(first, originalData);

    // Wait for TTL to expire
    await new Promise((resolve) => setTimeout(resolve, 20));

    // Second call fails but returns stale data
    shouldFail = true;
    const second = await cache.getBuckets(fetchFn);
    assert.deepStrictEqual(second, originalData, 'Should return stale data on fetch error');
  });

  test('no stale data available: throws error on first call failure', async () => {
    const cache = new SchemaCache(1000);
    const fetchFn = async () => {
      throw new Error('API unavailable');
    };

    await assert.rejects(
      () => cache.getBuckets(fetchFn),
      /API unavailable/,
      'Should throw when no stale data is available'
    );
  });
});

suite('SchemaCache - Tables', () => {
  test('cache hit within TTL: fetchFn is not called a second time', async () => {
    const cache = new SchemaCache(1000);
    let callCount = 0;
    const fetchFn = async () => {
      callCount++;
      return [{ name: 'users' }, { name: 'orders' }];
    };

    const first = await cache.getTables('in.c-main', fetchFn);
    const second = await cache.getTables('in.c-main', fetchFn);

    assert.strictEqual(callCount, 1, 'fetchFn should be called only once within TTL');
    assert.deepStrictEqual(first, second);
    assert.strictEqual(first.length, 2);
  });

  test('cache miss after TTL expiry: fetchFn is called again', async () => {
    const cache = new SchemaCache(10); // 10ms TTL
    let callCount = 0;
    const fetchFn = async () => {
      callCount++;
      return [{ name: `table-${callCount}` }];
    };

    await cache.getTables('in.c-main', fetchFn);
    assert.strictEqual(callCount, 1);

    // Wait for TTL to expire
    await new Promise((resolve) => setTimeout(resolve, 20));

    const result = await cache.getTables('in.c-main', fetchFn);
    assert.strictEqual(callCount, 2, 'fetchFn should be called again after TTL expiry');
    assert.deepStrictEqual(result, [{ name: 'table-2' }]);
  });

  test('stale-on-error: returns old cached data when fetch fails', async () => {
    const cache = new SchemaCache(10); // 10ms TTL
    const originalData = [{ name: 'users' }];
    let shouldFail = false;

    const fetchFn = async () => {
      if (shouldFail) {
        throw new Error('API unavailable');
      }
      return originalData;
    };

    // First call succeeds and populates cache
    const first = await cache.getTables('in.c-main', fetchFn);
    assert.deepStrictEqual(first, originalData);

    // Wait for TTL to expire
    await new Promise((resolve) => setTimeout(resolve, 20));

    // Second call fails but returns stale data
    shouldFail = true;
    const second = await cache.getTables('in.c-main', fetchFn);
    assert.deepStrictEqual(second, originalData, 'Should return stale data on fetch error');
  });

  test('no stale data available: throws error on first call failure', async () => {
    const cache = new SchemaCache(1000);
    const fetchFn = async () => {
      throw new Error('API unavailable');
    };

    await assert.rejects(
      () => cache.getTables('in.c-main', fetchFn),
      /API unavailable/,
      'Should throw when no stale data is available'
    );
  });

  test('per-bucket caching: different buckets have separate caches', async () => {
    const cache = new SchemaCache(1000);
    let callCount = 0;

    const fetchMain = async () => {
      callCount++;
      return [{ name: 'users' }];
    };
    const fetchOutput = async () => {
      callCount++;
      return [{ name: 'results' }];
    };

    const mainTables = await cache.getTables('in.c-main', fetchMain);
    const outputTables = await cache.getTables('out.c-output', fetchOutput);

    assert.strictEqual(callCount, 2, 'Each bucket should trigger its own fetch');
    assert.deepStrictEqual(mainTables, [{ name: 'users' }]);
    assert.deepStrictEqual(outputTables, [{ name: 'results' }]);

    // Second call to in.c-main should use cache
    await cache.getTables('in.c-main', fetchMain);
    assert.strictEqual(callCount, 2, 'Cached bucket should not trigger another fetch');
  });
});

suite('SchemaCache - Invalidation', () => {
  test('invalidateBucket clears only the specified bucket', async () => {
    const cache = new SchemaCache(1000);
    let callCount = 0;

    const fetchFn = async () => {
      callCount++;
      return [{ name: `table-${callCount}` }];
    };

    await cache.getTables('in.c-main', fetchFn);
    await cache.getTables('out.c-output', fetchFn);
    assert.strictEqual(callCount, 2);

    // Invalidate only in.c-main
    cache.invalidateBucket('in.c-main');

    // in.c-main should fetch again
    await cache.getTables('in.c-main', fetchFn);
    assert.strictEqual(callCount, 3, 'Invalidated bucket should trigger a new fetch');

    // out.c-output should still be cached
    await cache.getTables('out.c-output', fetchFn);
    assert.strictEqual(callCount, 3, 'Non-invalidated bucket should still use cache');
  });

  test('invalidate clears all cached data', async () => {
    const cache = new SchemaCache(1000);
    let bucketCallCount = 0;
    let tableCallCount = 0;

    const fetchBuckets = async () => {
      bucketCallCount++;
      return [{ id: 'in.c-main' }];
    };
    const fetchTables = async () => {
      tableCallCount++;
      return [{ name: 'users' }];
    };

    await cache.getBuckets(fetchBuckets);
    await cache.getTables('in.c-main', fetchTables);
    assert.strictEqual(bucketCallCount, 1);
    assert.strictEqual(tableCallCount, 1);

    // Invalidate all
    cache.invalidate();

    // Both should fetch again
    await cache.getBuckets(fetchBuckets);
    await cache.getTables('in.c-main', fetchTables);
    assert.strictEqual(bucketCallCount, 2, 'Buckets should be re-fetched after full invalidation');
    assert.strictEqual(tableCallCount, 2, 'Tables should be re-fetched after full invalidation');
  });

  test('invalidate on empty cache does not throw', () => {
    const cache = new SchemaCache(1000);
    assert.doesNotThrow(() => cache.invalidate());
  });

  test('invalidateBucket on non-existent bucket does not throw', () => {
    const cache = new SchemaCache(1000);
    assert.doesNotThrow(() => cache.invalidateBucket('nonexistent'));
  });
});
