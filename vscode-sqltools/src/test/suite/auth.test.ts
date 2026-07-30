import * as assert from 'assert';
import {
  buildAuthHeaders,
  buildBearerHeaders,
  detectAuthMode,
  fetchAccessibleProjects,
  mapDiscoveryError,
  parseAccessibleProjects,
  PAT_INVALID_MESSAGE,
  PAT_NOT_ENABLED_MESSAGE,
} from '../../auth';
import {
  AUTHORIZATION_HEADER,
  PAT_DISCOVERY_PATH,
  PAT_TOKEN_PREFIX,
  PROJECT_ID_HEADER,
  STORAGE_API_TOKEN_HEADER,
} from '../../constants';

/** Obvious placeholders -- never real credentials */
const STORAGE_TOKEN = '1234-storage-token-placeholder';
const PAT = `${PAT_TOKEN_PREFIX}placeholder-personal-access-token`;

suite('Auth - detectAuthMode', () => {
  test('recognises a Personal Access Token by its prefix', () => {
    assert.strictEqual(detectAuthMode(PAT), 'pat');
  });

  test('ignores surrounding whitespace', () => {
    assert.strictEqual(detectAuthMode(`  ${PAT}\n`), 'pat');
  });

  test('treats anything without the prefix as a Storage API token', () => {
    assert.strictEqual(detectAuthMode(STORAGE_TOKEN), 'storageApiToken');
  });

  test('the prefix match is case sensitive', () => {
    assert.strictEqual(detectAuthMode('KBC_PAT_placeholder'), 'storageApiToken');
  });

  test('a token containing the prefix elsewhere is not a PAT', () => {
    assert.strictEqual(detectAuthMode(`123-${PAT_TOKEN_PREFIX}suffix`), 'storageApiToken');
  });

  test('empty, null and undefined default to Storage API token', () => {
    assert.strictEqual(detectAuthMode(''), 'storageApiToken');
    assert.strictEqual(detectAuthMode(null), 'storageApiToken');
    assert.strictEqual(detectAuthMode(undefined), 'storageApiToken');
  });
});

suite('Auth - buildAuthHeaders', () => {
  test('Storage API token goes into the storage token header only', () => {
    const headers = buildAuthHeaders({ token: STORAGE_TOKEN });
    assert.strictEqual(headers[STORAGE_API_TOKEN_HEADER], STORAGE_TOKEN);
    assert.strictEqual(headers[AUTHORIZATION_HEADER], undefined);
    assert.strictEqual(headers[PROJECT_ID_HEADER], undefined);
    assert.strictEqual(Object.keys(headers).length, 1);
  });

  test('a project id is ignored for a Storage API token', () => {
    const headers = buildAuthHeaders({ token: STORAGE_TOKEN, projectId: '1234' });
    assert.deepStrictEqual(Object.keys(headers), [STORAGE_API_TOKEN_HEADER]);
  });

  test('PAT goes into the bearer and project headers only', () => {
    const headers = buildAuthHeaders({ token: PAT, projectId: '1234' });
    assert.strictEqual(headers[AUTHORIZATION_HEADER], `Bearer ${PAT}`);
    assert.strictEqual(headers[PROJECT_ID_HEADER], '1234');
    assert.strictEqual(headers[STORAGE_API_TOKEN_HEADER], undefined);
    assert.strictEqual(Object.keys(headers).length, 2);
  });

  test('PAT without a project id fails fast', () => {
    assert.throws(() => buildAuthHeaders({ token: PAT }), /Project ID is required/);
    assert.throws(() => buildAuthHeaders({ token: PAT, projectId: '  ' }), /Project ID is required/);
  });

  test('trims whitespace around both credential and project id', () => {
    const headers = buildAuthHeaders({ token: `  ${PAT}  `, projectId: ' 1234 ' });
    assert.strictEqual(headers[AUTHORIZATION_HEADER], `Bearer ${PAT}`);
    assert.strictEqual(headers[PROJECT_ID_HEADER], '1234');
  });

  test('project id beyond the 32-bit range is sent verbatim', () => {
    const bigProjectId = '9007199254740993';
    const headers = buildAuthHeaders({ token: PAT, projectId: bigProjectId });
    assert.strictEqual(headers[PROJECT_ID_HEADER], bigProjectId);
  });
});

suite('Auth - buildBearerHeaders', () => {
  test('carries the bearer credential and nothing else', () => {
    const headers = buildBearerHeaders(PAT);
    assert.deepStrictEqual(headers, { [AUTHORIZATION_HEADER]: `Bearer ${PAT}` });
  });
});

suite('Auth - parseAccessibleProjects', () => {
  test('reads projects from a single item', () => {
    const projects = parseAccessibleProjects({
      items: [
        {
          projects: [{ id: 1234, name: 'My Project' }],
        },
      ],
    } as any);
    assert.deepStrictEqual(projects, [{ id: '1234', name: 'My Project' }]);
  });

  test('unions projects across items and deduplicates by id', () => {
    const projects = parseAccessibleProjects({
      items: [
        { projects: [{ id: 1, name: 'Alpha' }, { id: 2, name: 'Beta' }] },
        { projects: [{ id: 2, name: 'Beta' }, { id: 3, name: 'Gamma' }] },
      ],
    } as any);
    assert.deepStrictEqual(projects, [
      { id: '1', name: 'Alpha' },
      { id: '2', name: 'Beta' },
      { id: '3', name: 'Gamma' },
    ]);
  });

  test('uses the resolved projects even when the scope is "all"', () => {
    const projects = parseAccessibleProjects({
      items: [
        {
          scope: { all: true },
          projects: [{ id: 42, name: 'Everything' }],
        },
      ],
    } as any);
    assert.deepStrictEqual(projects, [{ id: '42', name: 'Everything' }]);
  });

  test('empty items yields no projects', () => {
    assert.deepStrictEqual(parseAccessibleProjects({ items: [] }), []);
  });

  test('items without projects yields no projects', () => {
    assert.deepStrictEqual(parseAccessibleProjects({ items: [{}, { projects: [] }] }), []);
  });

  test('missing payload yields no projects', () => {
    assert.deepStrictEqual(parseAccessibleProjects(null), []);
    assert.deepStrictEqual(parseAccessibleProjects(undefined), []);
    assert.deepStrictEqual(parseAccessibleProjects({} as any), []);
  });

  test('a project id larger than 2^31 stays an exact string', () => {
    const projects = parseAccessibleProjects({
      items: [{ projects: [{ id: '21474836470', name: 'Big' }, { id: 3000000000, name: 'Also big' }] }],
    } as any);
    assert.deepStrictEqual(projects, [
      { id: '21474836470', name: 'Big' },
      { id: '3000000000', name: 'Also big' },
    ]);
  });

  test('skips entries without an id and labels unnamed projects', () => {
    const projects = parseAccessibleProjects({
      items: [{ projects: [{ name: 'No id' }, { id: 7 }] }],
    } as any);
    assert.deepStrictEqual(projects, [{ id: '7', name: 'Project 7' }]);
  });
});

suite('Auth - mapDiscoveryError', () => {
  test('404 reports that the stack lacks PAT support', () => {
    assert.strictEqual(mapDiscoveryError(404, '').message, PAT_NOT_ENABLED_MESSAGE);
  });

  test('401 reports an unusable token', () => {
    assert.strictEqual(mapDiscoveryError(401, '').message, PAT_INVALID_MESSAGE);
  });

  test('other statuses keep the API detail', () => {
    const error = mapDiscoveryError(500, JSON.stringify({ message: 'boom' }));
    assert.match(error.message, /HTTP 500: boom/);
  });

  test('non-JSON bodies are included as text', () => {
    const error = mapDiscoveryError(502, 'Bad Gateway');
    assert.match(error.message, /HTTP 502: Bad Gateway/);
  });
});

suite('Auth - fetchAccessibleProjects', () => {
  const originalFetch = globalThis.fetch;
  let lastUrl: string;
  let lastHeaders: Record<string, string>;

  function stubFetch(status: number, body: string): void {
    (globalThis as any).fetch = async (url: string, init: any) => {
      lastUrl = url;
      lastHeaders = init.headers;
      return {
        ok: status >= 200 && status < 300,
        status,
        text: async () => body,
        json: async () => JSON.parse(body),
      };
    };
  }

  teardown(() => {
    (globalThis as any).fetch = originalFetch;
  });

  test('calls the discovery route with the bearer credential only', async () => {
    stubFetch(200, JSON.stringify({ items: [{ projects: [{ id: 5, name: 'Solo' }] }] }));

    const projects = await fetchAccessibleProjects('connection.keboola.com', PAT);

    assert.strictEqual(lastUrl, `https://connection.keboola.com${PAT_DISCOVERY_PATH}`);
    assert.strictEqual(lastHeaders[AUTHORIZATION_HEADER], `Bearer ${PAT}`);
    assert.strictEqual(lastHeaders[PROJECT_ID_HEADER], undefined);
    assert.strictEqual(lastHeaders[STORAGE_API_TOKEN_HEADER], undefined);
    assert.deepStrictEqual(projects, [{ id: '5', name: 'Solo' }]);
  });

  test('404 is reported as the stack feature being disabled', async () => {
    stubFetch(404, '');
    await assert.rejects(
      fetchAccessibleProjects('connection.keboola.com', PAT),
      new RegExp(PAT_NOT_ENABLED_MESSAGE.split('.')[0])
    );
  });

  test('401 is reported as an unusable token', async () => {
    stubFetch(401, '');
    await assert.rejects(
      fetchAccessibleProjects('connection.keboola.com', PAT),
      /invalid, expired or revoked/
    );
  });
});
