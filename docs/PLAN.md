# VSCode SQLTools Adapter for Keboola

## Overview

Add a TypeScript VSCode SQLTools extension to the existing Keboola JDBC driver monorepo. Reorganize repo into `jdbc-driver/` and `vscode-sqltools/` subdirectories. Port stable JDBC driver features (v2.1.2 from main) to TypeScript. No experimental DuckDB/Kai features.

Reference implementation: Fisa's adapter at https://github.com/Vfisa/vscode-sql-tools-keboola-query-service-adapter (clone to `/tmp/keboola-sqltools-adapter` before starting).

## Key Requirements

- Stack URL: mandatory (dropdown with 5 predefined stacks + custom URL option)
- Token: mandatory
- Branch ID: OPTIONAL (auto-detected from Storage API if not provided)
- Workspace ID: OPTIONAL (auto-selected newest workspace if not provided)
- Query Service URL: auto-discovered from Storage API index, NOT hardcoded
- All constants in dedicated config, never hardcoded

## Target Structure

```
keboola_jdbc/
├── jdbc-driver/                # Java JDBC driver (moved from root)
│   ├── src/main/java/...
│   ├── src/test/java/...
│   ├── pom.xml
│   ├── Makefile
│   └── dist/
├── vscode-sqltools/            # NEW TypeScript SQLTools adapter
│   ├── src/
│   │   ├── extension.ts
│   │   ├── constants.ts
│   │   ├── types.ts
│   │   └── ls/
│   │       ├── plugin.ts
│   │       ├── driver.ts
│   │       ├── queries.ts
│   │       ├── schema-cache.ts
│   │       ├── virtual-tables.ts
│   │       └── help-command.ts
│   ├── src/test/suite/
│   ├── tests/
│   ├── icons/
│   ├── connection.schema.json
│   ├── ui.schema.json
│   ├── package.json
│   └── tsconfig.json
├── README.md
├── CLAUDE.md
├── .gitignore
└── .gitattributes
```

---

<!-- phase:0 branch:phase/0-monorepo-reorg base:main -->
## Phase 0: Monorepo Reorganization
<!-- tags: monorepo, restructure, git-mv, java, maven -->

### Description
Move the existing Java JDBC driver into a `jdbc-driver/` subdirectory. Update all root-level configuration files to reflect new paths. This phase must be completed and verified before any VSCode work begins.

### Branch
`phase/0-monorepo-reorg` (created from `main`, PR merges back to `main`)

### Files

**Move (git mv):**
- `src/` -> `jdbc-driver/src/`
- `pom.xml` -> `jdbc-driver/pom.xml`
- `Makefile` -> `jdbc-driver/Makefile`
- `dist/` -> `jdbc-driver/dist/`
- `dependency-reduced-pom.xml` -> `jdbc-driver/dependency-reduced-pom.xml`

**Delete:**
- `target/` (build artifact, regenerates)

**Modify:**
- `.gitignore` - prefix Java excludes with `jdbc-driver/`, add `vscode-sqltools/node_modules/`, `vscode-sqltools/out/`, `vscode-sqltools/*.vsix`, `vscode-sqltools/.vscode-test/`
- `.gitattributes` - change `dist/*.jar` to `jdbc-driver/dist/*.jar`
- `README.md` - rewrite as umbrella README linking to both subprojects
- `CLAUDE.md` - update all file paths with `jdbc-driver/` prefix

**No changes needed:**
- `jdbc-driver/pom.xml` - uses relative paths (src/ stays same relative to pom.xml)
- `jdbc-driver/Makefile` - uses relative paths

### Acceptance Criteria
- [ ] All Java source files are under `jdbc-driver/src/`
- [ ] `cd jdbc-driver && mvn test` passes with zero test failures
- [ ] `cd jdbc-driver && mvn clean package -DskipTests` produces uber-jar
- [ ] `.gitignore` excludes correct paths for both projects
- [ ] `.gitattributes` points LFS to `jdbc-driver/dist/*.jar`
- [ ] No files remain in root `src/` directory
- [ ] Git history is preserved (use `git mv`, not copy+delete)

### Dependencies
- None (first phase)

---

<!-- phase:1 branch:phase/1-vscode-scaffolding base:main -->
## Phase 1: VSCode Extension Scaffolding
<!-- tags: typescript, vscode, sqltools, scaffolding, npm, tsup -->

### Description
Create the VSCode SQLTools extension project structure with build system, connection form, and minimal boilerplate. The extension should compile and package into a .vsix, even though the driver is not yet functional. Clone Fisa's adapter to `/tmp/keboola-sqltools-adapter` first as reference.

### Branch
`phase/1-vscode-scaffolding` (created from `main` after Phase 0 merged, PR merges back to `main`)

### Files

**Create:**
- `vscode-sqltools/package.json` - Extension manifest. Publisher: `keboola`. Dependencies: `@sqltools/base-driver`, `@sqltools/types` (devDeps). Build with `tsup`. Register `keboola.cancelQuery` command in `contributes.commands`. Extension dependency on `mtxr.sqltools`. Activation events: `*`, `onLanguage:sql`, `onCommand:sqltools.*`.
- `vscode-sqltools/tsconfig.json` - target: `esnext`, module: `commonjs`, outDir: `out/`, resolveJsonModule: true, strict: true, sourceMap: true
- `vscode-sqltools/tsconfig.test.json` - target: `es2020`, module: `commonjs`, strict: false, includes `src/test/**/*`
- `vscode-sqltools/.vscode-test.mjs` - Test config: files `out/test/suite/**/*.test.js`, mocha ui `tdd`, timeout 10000
- `vscode-sqltools/.vscodeignore` - Exclude `src/`, `*.ts`, `tsconfig*`, `node_modules/`, `.vscode-test/`, `tests/`
- `vscode-sqltools/.gitignore` - `node_modules/`, `out/`, `*.vsix`, `.vscode-test/`
- `vscode-sqltools/connection.schema.json` - JSON Schema Draft 7. Properties: `name` (string, default "Keboola"), `keboolaStack` (enum: 5 stacks + "custom"), `customConnectionUrl` (string, optional), `token` (string, minLength 1), `branchId` (string, optional), `workspaceId` (string, optional). Required: `["keboolaStack", "token"]`. Do NOT include `name` in required (SQLTools adds it).
- `vscode-sqltools/ui.schema.json` - Field ordering, password widget for token, help text for each field explaining where to find values in Keboola
- `vscode-sqltools/src/extension.ts` - VSCode entry point. Acquire SQLTools extension, register plugin with icons, schemas, and LS plugin path. Register `keboola.cancelQuery` command via LS request. Pattern: copy from Fisa's `src/extension.ts`.
- `vscode-sqltools/src/ls/plugin.ts` - Language server plugin. Register driver class for each alias. Register `keboola/cancelQuery` request handler. Pattern: copy from Fisa's `src/ls/plugin.ts`.
- `vscode-sqltools/src/ls/queries.ts` - Minimal IBaseQueries stubs (all `SELECT 1`). Actual metadata comes from REST API, not SQL. Pattern: copy from Fisa's `src/ls/queries.ts`.
- `vscode-sqltools/src/constants.ts` - All configuration constants:
  - `DRIVER_ALIASES`: `[{ displayName: 'Keboola', value: 'Keboola' }]`
  - `KEBOOLA_STACKS`: Map of connection URLs to query URLs (fallback for auto-discovery)
  - `getConnectionUrl(keboolaStack, customConnectionUrl?)`: resolve effective connection URL
  - `getQueryUrl(connectionUrl)`: derive query URL via prefix replacement (fallback)
  - `QUERY_POLLING`: `{ INITIAL_INTERVAL_MS: 100, MAX_INTERVAL_MS: 2000, BACKOFF_MULTIPLIER: 1.5, DEFAULT_TIMEOUT_S: 300 }`
  - `DEFAULT_PAGE_SIZE = 1000`
  - `MAX_RESULT_ROWS = 10000`
  - `SCHEMA_CACHE_TTL_MS = 60000`
  - `HTTP_TIMEOUT_MS = 30000`
  - `MAX_RETRIES = 3`
  - `VIRTUAL_TABLE_DEFAULT_LIMIT = 100`
  - `TERMINAL_STATES = new Set(['completed', 'failed', 'canceled'])`
  - `STATEMENT_TERMINAL_STATES = new Set(['completed', 'failed', 'canceled', 'notExecuted'])`
- `vscode-sqltools/src/types.ts` - TypeScript interfaces:
  - `KeboolaCredentials { keboolaStack: string; customConnectionUrl?: string; token: string; branchId?: string; workspaceId?: string }`
  - `QueryJobResponse { queryJobId: string; sessionId?: string }`
  - `JobStatusResponse { queryJobId: string; status: string; statements: StatementStatusResponse[] }`
  - `StatementStatusResponse { id: string; status: string; error?: { message: string }; numberOfRows?: number }`
  - `QueryResultResponse { columns: { name: string; type?: string }[]; data: any[][]; numberOfRows?: number }`
  - `BranchResponse { id: number; name: string; isDefault: boolean }`
  - `WorkspaceResponse { id: number; name: string; type: string }`
  - `BucketInfo { id: string; name: string; stage: string; description?: string }`
  - `TableInfo { id: string; name: string; primaryKey?: string[] }`
- `vscode-sqltools/icons/` - Copy 4 PNG files from Fisa's `/tmp/keboola-sqltools-adapter/icons/` (active.png, default.png, inactive.png, keboola-logo.png)

**Create (tests):**
- `vscode-sqltools/src/test/suite/constants.test.ts` - Tests for `getConnectionUrl()` (predefined stack, custom stack, missing URL error) and `getQueryUrl()` (all predefined stacks, custom prefix replacement, invalid URL rejection). Pattern: port from Fisa's `src/test/suite/constants.test.ts`.
- `vscode-sqltools/src/test/suite/schema.test.ts` - JSON Schema validation with `ajv`. Tests: valid full payload, valid with optional branchId/workspaceId omitted, missing required fields, invalid enum. REGRESSION test: predefined stack with empty customConnectionUrl must pass. Pattern: port from Fisa's `src/test/suite/schema.test.ts`.

### Acceptance Criteria
- [ ] `cd vscode-sqltools && npm install` succeeds
- [ ] `cd vscode-sqltools && npm run compile` produces `out/extension.js` and `out/ls/plugin.js`
- [ ] `cd vscode-sqltools && npm run package` produces `.vsix` file
- [ ] All constants are in `constants.ts`, no hardcoded values elsewhere
- [ ] `branchId` and `workspaceId` are NOT in `required` array in `connection.schema.json`
- [ ] Unit tests pass for constants and schema validation

### Dependencies
- Phase 0

---

<!-- phase:2 branch:phase/2-core-driver base:main -->
## Phase 2: Core Driver with Auto-Discovery
<!-- tags: typescript, driver, api, query-service, storage-api, auto-discovery, pagination -->

### Description
Implement the main driver class with connection setup (auto-discovery of Query Service URL, branch, workspace), SQL query execution with pagination, and database explorer (sidebar). This is the largest and most critical phase.

### Branch
`phase/2-core-driver` (created from `main` after Phase 1 merged, PR merges back to `main`)

### Reference files from JDBC driver
- `jdbc-driver/src/main/java/com/keboola/jdbc/KeboolaConnection.java` - connection setup flow
- `jdbc-driver/src/main/java/com/keboola/jdbc/http/QueryServiceClient.java` - API calls
- `jdbc-driver/src/main/java/com/keboola/jdbc/config/DriverConfig.java` - constants

### Files

**Create:**
- `vscode-sqltools/src/ls/driver.ts` (~600 LOC) - Main driver class extending `AbstractDriver`:
  - **`open()`**: (1) resolve connectionUrl from keboolaStack/customConnectionUrl, (2) auto-discover Query Service URL via `GET https://{connectionUrl}/v2/storage` -> parse `services[]` where `id === "query"` -> extract hostname, fallback to `connection.` -> `query.` prefix replacement, (3) if branchId not provided: `GET /v2/storage/dev-branches` -> find `isDefault === true`, (4) if workspaceId not provided: `GET /v2/storage/workspaces` -> select last (newest), (5) generate UUID sessionId, (6) validate both APIs
  - **`close()`**: cleanup state, remove from instance registry
  - **`testConnection()`**: call `open()` then `query('SELECT 1')`
  - **`query(sql)`**: (1) check for special commands (placeholder, return early if matched), (2) submit job via `POST /api/v1/branches/{b}/workspaces/{w}/queries` with `{ statements: [sql], sessionId }`, (3) poll with exponential backoff from constants, (4) fetch results with pagination (1000 rows/page, all pages up to MAX_RESULT_ROWS), (5) map to `NSDatabase.IResult[]`
  - **`getChildrenForItem()`**: CONNECTION -> buckets (Storage API `/v2/storage/buckets`), SCHEMA -> "Tables" resource group, RESOURCE_GROUP -> tables (`/v2/storage/buckets/{id}/tables`), TABLE -> columns with types (`/v2/storage/tables/{id}` -> `definition.columns` -> `columnMetadata` fallback, default STRING)
  - **`searchItems()`**: search tables across buckets by name
  - **`getStaticCompletions()`**: minimal SQL keywords (expanded in Phase 7)
  - **HTTP helpers**: `storageApiRequest(path)`, `queryApiRequest(path)`, `apiRequest(baseUrl, path, options)` with `X-StorageApi-Token` header, retry 3x for 5xx/429 with exponential backoff, fail fast for 401/403/400, error mapping (401 -> "Invalid or expired token", 403 -> "Access denied", 404 -> "Branch or workspace not found", 5xx -> "Service error")

**Create (tests):**
- `vscode-sqltools/src/test/suite/driver.test.ts` - Unit tests with mocked HTTP:
  - Auto-discovery of query service URL from services array
  - Auto-detection of default branch from dev-branches
  - Auto-selection of newest workspace
  - Polling with timeout detection
  - Multi-page result assembly
  - Error mapping for each HTTP status code
  - Retry logic for 5xx responses

### Acceptance Criteria
- [ ] Extension compiles with driver
- [ ] Driver can be instantiated with credentials
- [ ] `open()` resolves query URL, branch, and workspace automatically
- [ ] `query('SELECT 1')` returns valid `NSDatabase.IResult`
- [ ] Result pagination fetches multiple pages and assembles them
- [ ] Sidebar explorer shows buckets -> tables -> columns with types
- [ ] HTTP errors produce user-friendly messages
- [ ] 5xx/429 responses are retried up to 3 times
- [ ] Unit tests pass

### Dependencies
- Phase 1

---

<!-- phase:3 branch:phase/3-schema-cache base:main -->
## Phase 3: Schema Cache
<!-- tags: typescript, cache, ttl, stale-on-error, performance -->

### Description
Implement a metadata cache with 60-second TTL and stale-on-error fallback for sidebar performance. Port from JDBC driver's `SchemaCache.java`.

### Branch
`phase/3-schema-cache` (created from `main` after Phase 2 merged, PR merges back to `main`)

### Reference
- `jdbc-driver/src/main/java/com/keboola/jdbc/meta/SchemaCache.java`

### Files

**Create:**
- `vscode-sqltools/src/ls/schema-cache.ts` - SchemaCache class:
  - `getBuckets(fetchFn)`: check cache timestamp, if within TTL return cached data. If expired, call fetchFn. On fetch error: return stale data if available, throw if not. Update timestamp on success.
  - `getTables(bucketId, fetchFn)`: same pattern, per-bucket Map cache
  - `invalidate()`: clear all cached data
  - `invalidateBucket(bucketId)`: clear specific bucket
  - No locking needed (Node.js single-threaded)
  - TTL from `SCHEMA_CACHE_TTL_MS` constant (60000ms)

**Modify:**
- `vscode-sqltools/src/ls/driver.ts` - Replace direct API calls in `getChildrenForItem()` with SchemaCache calls

**Create (tests):**
- `vscode-sqltools/src/test/suite/schema-cache.test.ts`:
  - Cache hit within TTL (fetchFn not called)
  - Cache miss after TTL (fetchFn called)
  - Stale-on-error (fetch fails, returns old data)
  - No stale data available (throws error)
  - Per-bucket invalidation
  - Full invalidation

### Acceptance Criteria
- [ ] Sidebar explorer uses cached data within 60s TTL
- [ ] After TTL expiry, fresh data is fetched
- [ ] If API fails and stale data exists, stale data is returned
- [ ] `invalidate()` clears all caches
- [ ] Unit tests pass

### Dependencies
- Phase 2

---

<!-- phase:4 branch:phase/4-use-schema base:main -->
## Phase 4: USE SCHEMA/DATABASE and Session Persistence
<!-- tags: typescript, sql-interception, multi-statement, session, snowflake -->

### Description
Intercept USE SCHEMA/DATABASE commands and implement multi-statement jobs with sessionId for server-side session persistence.

### Branch
`phase/4-use-schema` (created from `main` after Phase 2 merged, PR merges back to `main`)

### Reference
- `jdbc-driver/src/main/java/com/keboola/jdbc/KeboolaStatement.java` (USE_PATTERN, buildStatements)

### Files

**Modify:**
- `vscode-sqltools/src/ls/driver.ts`:
  - Add `currentSchema: string | null` instance field
  - Add USE detection regex: `^\s*USE\s+(?:SCHEMA|DATABASE)?\s*"?([^"\s;]+)"?\s*;?\s*$`
  - In `query()`: detect USE commands, update `currentSchema`, still send to server
  - When `currentSchema` is set and executing non-USE queries: submit as multi-statement `["USE SCHEMA \"xxx\"", "<actual query>"]`
  - Ensure `sessionId` is included in every submit body

**Create (tests):**
- Add to `vscode-sqltools/src/test/suite/driver.test.ts`:
  - USE SCHEMA regex detection and local state update
  - USE DATABASE regex detection
  - Multi-statement job body when schema is set
  - sessionId present in all submit requests
  - Quoted schema names handled correctly

### Acceptance Criteria
- [ ] `USE SCHEMA "my_schema"` updates local state and executes on server
- [ ] Subsequent queries prepend `USE SCHEMA` in statements array
- [ ] `sessionId` is a UUID generated per connection in `open()`
- [ ] `sessionId` is included in every query submission
- [ ] Unit tests pass

### Dependencies
- Phase 2

---

<!-- phase:5 branch:phase/5-virtual-tables base:main -->
## Phase 5: Virtual Tables and KEBOOLA HELP
<!-- tags: typescript, virtual-tables, keboola-api, sql-interception, help -->

### Description
Implement virtual `_keboola.*` tables and the KEBOOLA HELP command. These intercept SQL before sending to Query Service and return locally assembled data from Keboola APIs.

### Branch
`phase/5-virtual-tables` (created from `main` after Phase 2 merged, PR merges back to `main`)

### Reference
- `jdbc-driver/src/main/java/com/keboola/jdbc/command/VirtualTableHandler.java`
- `jdbc-driver/src/main/java/com/keboola/jdbc/command/VirtualTableMetadata.java`
- `jdbc-driver/src/main/java/com/keboola/jdbc/command/HelpCommandHandler.java`

### Files

**Create:**
- `vscode-sqltools/src/ls/virtual-tables.ts`:
  - `canHandle(sql)`: regex `SELECT\s+.*\bFROM\s+["']?_keboola["']?\.["']?(\w+)["']?` (handles quoting)
  - `execute(sql, apiHelpers)`: parse table name and LIMIT, fetch data, return `NSDatabase.IResult[]`
  - Table implementations:
    - `_keboola.components`: `GET /v2/storage/components` -> flatten components + configs. Columns: component_id, component_name, component_type, config_id, config_name, config_description, version, created, is_disabled
    - `_keboola.events`: `GET /v2/storage/events?limit=N`. Columns: event_id, type, component, message, created
    - `_keboola.tables`: `GET /v2/storage/tables?include=columns,buckets`. Columns: table_id, bucket_id, name, primary_key, rows_count, data_size_bytes, last_import_date, created
    - `_keboola.buckets`: `GET /v2/storage/buckets`. Columns: bucket_id, name, stage, description, tables_count, data_size_bytes, created
    - `_keboola.jobs`: `GET /v2/storage/jobs?limit=N` (or Job Queue API). Columns: job_id, component_id, config_id, status, created, started, finished, duration_sec
  - `parseLimitFromSql(sql)`: extract `LIMIT N`, default to `VIRTUAL_TABLE_DEFAULT_LIMIT` (100)

- `vscode-sqltools/src/ls/help-command.ts`:
  - `canHandle(sql)`: regex `^\s*KEBOOLA\s+HELP\s*;?\s*$`
  - `execute()`: return `NSDatabase.IResult` with columns: Command, Syntax, Description
  - List only SQLTools-relevant commands (no DuckDB/Kai/Push/Pull):
    - KEBOOLA HELP, SHOW HISTORY, USE SCHEMA, USE DATABASE, virtual tables overview

**Modify:**
- `vscode-sqltools/src/ls/driver.ts` - In `query()`, before sending to Query Service: check `helpCommand.canHandle(sql)` and `virtualTables.canHandle(sql)`. If matched, return their result directly.

**Create (tests):**
- `vscode-sqltools/src/test/suite/virtual-tables.test.ts`:
  - Pattern detection for various quoting styles
  - LIMIT parsing (explicit, missing/default)
  - Help command pattern detection
  - Result format validation

### Acceptance Criteria
- [ ] `SELECT * FROM _keboola.components LIMIT 5` returns component data
- [ ] `SELECT * FROM "_keboola"."events"` handles quoted identifiers
- [ ] Default LIMIT 100 applied when not specified
- [ ] `KEBOOLA HELP` lists available commands
- [ ] Virtual table queries do NOT hit the Query Service API
- [ ] Unit tests pass

### Dependencies
- Phase 2

---

<!-- phase:6 branch:phase/6-history-cancel base:main -->
## Phase 6: SHOW HISTORY and Query Cancellation
<!-- tags: typescript, query-history, cancellation, query-service-api -->

### Description
Add workspace query history command and query cancellation support. Both features are ported from Fisa's adapter.

### Branch
`phase/6-history-cancel` (created from `main` after Phase 2 merged, PR merges back to `main`)

### Reference
- Fisa's `src/ls/driver.ts` (fetchWorkspaceQueryHistory, cancelQuery, cancelAllActiveQueries)

### Files

**Modify:**
- `vscode-sqltools/src/ls/driver.ts`:
  - In `query()`: detect `SHOW HISTORY` / `SHOW QUERY HISTORY` (case-insensitive, trim semicolons)
  - `fetchWorkspaceQueryHistory()`: `GET /api/v1/branches/{b}/workspaces/{w}/queries?pageSize=500` -> map to IResult with columns: Job ID, Status, Query, Created At, Completed At
  - `cancelQuery(queryJobId)`: `POST /api/v1/queries/{queryJobId}/cancel` with `{ reason: "Cancelled by user from VS Code" }`
  - Static `cancelAllActiveQueries()`: iterate all driver instances, cancel active jobs
  - Track `activeQueryJobId` per instance, clear in finally block

### Acceptance Criteria
- [ ] `SHOW HISTORY` returns table with past query jobs
- [ ] `SHOW QUERY HISTORY` works as alias
- [ ] Cancel command stops running queries
- [ ] Multiple concurrent connections can each be cancelled independently
- [ ] Timed-out queries are automatically cancelled

### Dependencies
- Phase 2

---

<!-- phase:7 branch:phase/7-polish base:main -->
## Phase 7: Autocomplete and Polish
<!-- tags: typescript, autocomplete, completions, readme, documentation -->

### Description
Add comprehensive SQL keyword completions including Keboola-specific commands and virtual table names. Create extension README. Final polish.

### Branch
`phase/7-polish` (created from `main` after Phases 3-6 merged, PR merges back to `main`)

### Files

**Modify:**
- `vscode-sqltools/src/ls/driver.ts` - Expand `getStaticCompletions()`:
  - Standard SQL keywords (SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, etc.)
  - Keboola commands: `KEBOOLA HELP`, `SHOW HISTORY` (sorted to top with `sortText: "0000"`)
  - Schema commands: `USE SCHEMA`, `USE DATABASE`
  - Virtual table completions: `_keboola.components`, `_keboola.events`, `_keboola.jobs`, `_keboola.tables`, `_keboola.buckets` (with descriptions)

**Create:**
- `vscode-sqltools/README.md` - Extension documentation:
  - Features overview
  - Prerequisites (VSCode, SQLTools, Keboola project)
  - Installation from VSIX
  - Connection configuration (with stack dropdown screenshot reference)
  - Auto-detection of branch and workspace
  - Usage: running queries, browsing sidebar, SHOW HISTORY, virtual tables, KEBOOLA HELP, query cancellation
  - Development: build, test, package commands

### Acceptance Criteria
- [ ] SQL keywords appear in autocomplete
- [ ] KEBOOLA HELP and SHOW HISTORY appear at top of completions
- [ ] Virtual table names appear in completions with descriptions
- [ ] README documents all features

### Dependencies
- Phase 3, Phase 4, Phase 5, Phase 6

---

<!-- phase:8 branch:phase/8-integration-tests base:main -->
## Phase 8: Integration Tests
<!-- tags: typescript, integration-tests, live-api, keboola-token -->

### Description
Create standalone integration tests that validate the full driver against live Keboola APIs. These run outside VSCode context using direct HTTP calls.

### Branch
`phase/8-integration-tests` (created from `main` after Phase 7 merged, PR merges back to `main`)

### Files

**Create:**
- `vscode-sqltools/tests/sample.env` - Template for test credentials:
  ```
  KEBOOLA_STACK=connection.keboola.com
  KEBOOLA_CUSTOM_CONNECTION_URL=
  KEBOOLA_TOKEN=your-token-here
  KEBOOLA_BRANCH_ID=
  KEBOOLA_WORKSPACE_ID=
  KEBOOLA_TEST_QUERY=SELECT 1
  ```
- `vscode-sqltools/tests/integration.test.ts` - Live API tests:
  1. Storage API connectivity: `GET /v2/storage/buckets` returns array
  2. Auto-discovery: `GET /v2/storage` contains services with query URL
  3. Branch auto-detection: `GET /v2/storage/dev-branches` has default branch
  4. Workspace auto-selection: `GET /v2/storage/workspaces` returns workspaces
  5. Query execution: submit `SELECT 1`, poll, fetch results
  6. Pagination: query returning >1000 rows fetches multiple pages
  7. SHOW HISTORY: returns entries with expected columns
  8. Virtual tables: `_keboola.buckets` returns data
  9. Error handling: invalid token returns 401

### Acceptance Criteria
- [ ] `npx tsx tests/integration.test.ts` passes with valid credentials
- [ ] Tests skip gracefully when credentials not available
- [ ] `tests/sample.env` contains only placeholders, no real tokens
- [ ] All API endpoints are exercised

### Dependencies
- Phase 7

---

## Phase Dependencies

```
Phase 0 (phase/0-monorepo-reorg) -> PR -> main
  |
  v
Phase 1 (phase/1-vscode-scaffolding) -> PR -> main
  |
  v
Phase 2 (phase/2-core-driver) -> PR -> main
  |
  ├──> Phase 3 (phase/3-schema-cache) -> PR -> main       ┐
  ├──> Phase 4 (phase/4-use-schema) -> PR -> main          ├─ parallel
  ├──> Phase 5 (phase/5-virtual-tables) -> PR -> main      │
  └──> Phase 6 (phase/6-history-cancel) -> PR -> main      ┘
         |
         v
Phase 7 (phase/7-polish) -> PR -> main
  |
  v
Phase 8 (phase/8-integration-tests) -> PR -> main
```

Each phase creates a branch from current `main`, implements the feature, opens a PR, and merges back to `main` before the next dependent phase starts.

## Key Differences from Fisa's Adapter

| Aspect | Fisa | Our implementation |
|--------|------|-------------------|
| Query Service URL | Hardcoded map | Auto-discovery from /v2/storage |
| Branch ID | Required | Optional, auto-detected |
| Workspace ID | Required | Optional, auto-detected |
| Result page size | 500, single page | 1000, all pages up to limit |
| SchemaCache | None | 60s TTL + stale-on-error |
| USE SCHEMA | Not supported | Multi-statement + sessionId |
| Session persistence | None | UUID sessionId per connection |
| Virtual tables | Not supported | 5 _keboola.* tables |
| KEBOOLA HELP | Not supported | Command listing |
| HTTP retry | None | 3x for 5xx/429 |
| Polling start | 200ms | 100ms (matches JDBC) |

## Reference Files (JDBC Driver)

- `jdbc-driver/src/main/java/com/keboola/jdbc/KeboolaConnection.java` - connection setup, auto-discovery, branch/workspace resolve
- `jdbc-driver/src/main/java/com/keboola/jdbc/http/QueryServiceClient.java` - submit, poll, fetch API
- `jdbc-driver/src/main/java/com/keboola/jdbc/meta/SchemaCache.java` - cache pattern with TTL + stale-on-error
- `jdbc-driver/src/main/java/com/keboola/jdbc/meta/TypeMapper.java` - Snowflake type mapping
- `jdbc-driver/src/main/java/com/keboola/jdbc/config/DriverConfig.java` - all constants
- `jdbc-driver/src/main/java/com/keboola/jdbc/command/VirtualTableHandler.java` - virtual table interception
- `jdbc-driver/src/main/java/com/keboola/jdbc/command/VirtualTableMetadata.java` - virtual table schema definitions
- `jdbc-driver/src/main/java/com/keboola/jdbc/command/HelpCommandHandler.java` - help command

## Verification Checklist

1. `cd jdbc-driver && mvn test` - JDBC tests pass after reorg
2. `cd vscode-sqltools && npm install && npm run compile` - builds
3. `cd vscode-sqltools && npm test` - unit tests pass
4. `cd vscode-sqltools && npm run package` - produces .vsix
5. Install .vsix in VSCode -> "Keboola" appears in SQLTools
6. Connect with token + stack -> auto-detects branch/workspace
7. `SELECT 1` -> result in panel
8. Sidebar: buckets -> tables -> columns with types
9. `SHOW HISTORY` -> query history table
10. `SELECT * FROM _keboola.components LIMIT 5` -> virtual table data
11. `KEBOOLA HELP` -> command listing
12. Cancel query from Command Palette
