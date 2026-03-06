# Keboola JDBC Driver

JDBC driver for [Keboola](https://www.keboola.com/) that connects any JDBC-compatible client (DBeaver, DataGrip, etc.) to Keboola projects via the Query Service API. Supports an embedded **DuckDB local backend** for offline SQL development with runtime backend switching.

User provides only an **API token and stack URL** — the driver auto-discovers branches, workspaces, and metadata from Snowflake directly. Or use the DuckDB backend with **zero cloud dependency**.

## JDBC Mapping

| JDBC Concept | Snowflake Concept | Example |
|---|---|---|
| Catalog | Database | `SAPI_901` |
| Schema | Schema | `WORKSPACE_1296474964` |
| Table | Table / View | `my_table` |

All metadata (databases, schemas, tables, columns) comes from Snowflake SHOW commands via the Query Service. The virtual `_keboola` schema provides Keboola platform metadata alongside real Snowflake objects.

## Quick Start

### Build

```bash
make dist
```

Produces an uber-jar at `dist/keboola-jdbc-driver-3.0.1-experimental.jar` (~78 MB, all dependencies shaded + embedded DuckDB).

Requires **Java 11+**.

### Connection Properties

| Property | Required | Default | Description |
|---|---|---|---|
| `token` | Yes* | — | Keboola Storage API token (*not required for DuckDB-only mode) |
| `branch` | No | auto-detect | Branch ID (auto-detects default branch) |
| `workspace` | No | auto-select | Workspace ID (auto-selects newest available) |
| `schema` | No | — | Default schema (e.g. `in.c-main`) for unqualified table refs |
| `backend` | No | `queryservice` | Backend type: `queryservice` or `duckdb` |
| `duckdbPath` | No | `:memory:` | DuckDB file path for persistent storage |

### JDBC URL Format

```
jdbc:keboola://<host>
```

Examples:
```
jdbc:keboola://connection.keboola.com
jdbc:keboola://connection.north-europe.azure.keboola.com
jdbc:keboola://localhost                                    # DuckDB-only mode
```

### Java Example

```java
// Query Service (cloud) — default
String url = "jdbc:keboola://connection.keboola.com";
Properties props = new Properties();
props.setProperty("token", System.getenv("KEBOOLA_TOKEN"));

try (Connection conn = DriverManager.getConnection(url, props)) {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT * FROM \"in.c-main\".\"items_catalog\" LIMIT 10")) {
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
}

// DuckDB (local, no cloud) — zero setup
String localUrl = "jdbc:keboola://localhost";
Properties localProps = new Properties();
localProps.setProperty("backend", "duckdb");
localProps.setProperty("duckdbPath", "/tmp/my_project.duckdb");

try (Connection conn = DriverManager.getConnection(localUrl, localProps)) {
    try (Statement stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE test (id INT, name VARCHAR)");
        stmt.execute("INSERT INTO test VALUES (1, 'hello')");
        ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        while (rs.next()) {
            System.out.println(rs.getString("name"));
        }
    }
}
```

## DBeaver Setup

1. **Database** > **Driver Manager** > **New**
2. Set **Driver Name** to `Keboola`
3. **Libraries** tab > **Add File** > select `dist/keboola-jdbc-driver-3.0.1-experimental.jar`
4. Set **Class Name** to `com.keboola.jdbc.KeboolaDriver`
5. Set **URL Template** to `jdbc:keboola://connection.keboola.com`
6. **OK** > **New Database Connection** > select `Keboola`
7. In **Driver Properties**, set `token` to your Keboola API token
8. **Test Connection**

The database navigator will show your Snowflake databases, schemas, tables, and views — plus the virtual `_keboola` schema with platform metadata.

### DBeaver with DuckDB backend

Same driver setup, but in **Driver Properties**:
- Set `backend` to `duckdb`
- Set `duckdbPath` to a file path (e.g. `/Users/you/project/local.duckdb`)
- Change **URL Template** to `jdbc:keboola://localhost`
- `token` is not required for DuckDB-only mode

## DuckDB Local Backend

The driver embeds DuckDB as a local SQL backend. Use it for offline SQL development, prototyping, and data exploration without cloud connectivity.

### Modes

| Mode | URL | Properties | Description |
|---|---|---|---|
| **DuckDB only** | `jdbc:keboola://localhost` | `backend=duckdb` | Local-only, no cloud |
| **DuckDB persistent** | `jdbc:keboola://localhost` | `backend=duckdb`, `duckdbPath=/path/to/file.duckdb` | Data persists between sessions |
| **Hybrid** | `jdbc:keboola://connection.keboola.com` | `token=xxx`, `backend=duckdb`, `duckdbPath=/path/to/file.duckdb` | Both backends initialized, switch at runtime |

### Runtime Backend Switching

When both `token` and `backend=duckdb` are provided, both backends are initialized. Switch between them at runtime:

```sql
-- Start in DuckDB (local)
CREATE TABLE staging (id INT, name VARCHAR);
INSERT INTO staging VALUES (1, 'local data');
SELECT * FROM staging;

-- Switch to Query Service (cloud)
KEBOOLA USE BACKEND queryservice;
SELECT * FROM "in.c-main"."items_catalog" LIMIT 10;

-- Switch back to DuckDB
KEBOOLA USE BACKEND duckdb;
SELECT * FROM staging;
```

### PULL — Cloud to Local

Pull data from Keboola Query Service into your local DuckDB:

```sql
-- Pull a table
KEBOOLA PULL TABLE "in.c-main"."items_catalog" INTO local_items;

-- Pull a custom query result
KEBOOLA PULL QUERY SELECT id, name FROM "in.c-main"."items_catalog" WHERE active = true LIMIT 1000 INTO active_items;
```

The `PULL` command executes the query on the Query Service backend and creates a local DuckDB table with the results. Requires both backends to be available (token must be provided).

### PUSH — Local to Cloud

Push data from your local DuckDB table to Keboola Query Service (Snowflake):

```sql
-- Push a local table (creates table in the current cloud schema)
KEBOOLA PUSH TABLE local_items;

-- Push to a specific target
KEBOOLA PUSH TABLE local_items INTO "out.c-results"."exported_items";
```

The `PUSH` command reads all rows from the local DuckDB table, creates a `VARCHAR`-typed table on Snowflake via the Query Service, and inserts data in batches. Requires both backends to be available.

### Session Logging

All SQL executed through the driver is logged to a DuckDB system table for dialect compatibility analysis:

```sql
-- View session log
KEBOOLA SESSION LOG;

-- Query the log table directly (when using DuckDB backend)
SELECT * FROM _keboola_session_log ORDER BY executed_at DESC LIMIT 20;
```

The session log captures: `executed_at`, `backend`, `sql_text`, `success`, `error_message`, `duration_ms`, `rows_affected`. Logging is automatic when a DuckDB backend is available.

## Virtual Tables

The driver exposes Keboola platform metadata as virtual SQL tables in the `_keboola` schema. These tables are backed by Keboola APIs — no Snowflake queries involved, instant results.

### Available virtual tables

| Virtual table | Description |
|---|---|
| `_keboola.components` | All component configurations with creator, version, and status |
| `_keboola.events` | Recent storage events (supports `LIMIT`) |
| `_keboola.jobs` | Recent jobs with status and duration (supports `LIMIT`) |
| `_keboola.tables` | All tables with row counts and sizes |
| `_keboola.buckets` | All buckets with metadata |

```sql
-- See what's available
KEBOOLA HELP;

-- All component configurations (joinable with jobs via config_id)
SELECT * FROM _keboola.components;

-- Which configs ran recently?
SELECT c.component_name, c.config_name, j.status, j.started
FROM _keboola.components c
JOIN _keboola.jobs j ON c.component_id = j.component_id AND c.config_id = j.config_id;

-- Last 10 storage events
SELECT * FROM _keboola.events LIMIT 10;

-- Recent job runs with status
SELECT * FROM _keboola.jobs LIMIT 20;

-- All tables with sizes
SELECT * FROM _keboola.tables;
```

### IDE integration

The `_keboola` schema appears in the DBeaver/DataGrip sidebar alongside real Snowflake schemas:

```
SAPI_901 (database)
  _keboola               <-- virtual schema
    Tables
      buckets
      components
      events
      jobs
      tables
  WORKSPACE_1296474964   <-- real Snowflake schemas
  in.c-main
  out.c-results
```

Autocomplete works for virtual table and column names. "View Data" from the sidebar context menu works too.

### How it works

The command dispatcher intercepts SQL in `KeboolaStatement.execute()` *before* it reaches the backend. If the SQL matches a virtual table pattern (`_keboola.*`) or a command (`KEBOOLA HELP`), it returns an in-memory `ArrayResultSet` directly — zero network calls.

```
SQL input
  |
  +-- "KEBOOLA HELP"                  --> HelpCommandHandler --> ArrayResultSet
  +-- "KEBOOLA USE BACKEND duckdb"    --> BackendSwitchHandler --> switch backend
  +-- "KEBOOLA PULL TABLE ..."        --> PullCommandHandler --> cloud-to-local transfer
  +-- "KEBOOLA PUSH TABLE ..."        --> PushCommandHandler --> local-to-cloud transfer
  +-- "KEBOOLA SESSION LOG"           --> SessionLogHandler --> ArrayResultSet
  +-- "_keboola.components"           --> VirtualTableHandler --> Storage API --> ArrayResultSet
  +-- "_keboola.jobs LIMIT 5"         --> VirtualTableHandler --> Job Queue API --> ArrayResultSet
  +-- "SELECT * FROM ..."             --> active backend (Query Service or DuckDB)
```

## USE SCHEMA Support

The driver supports setting a default schema so you can use unqualified table names:

```sql
USE SCHEMA "in.c-main";
SELECT * FROM "items_catalog" LIMIT 10;
-- No need to write "in.c-main"."items_catalog" -- Snowflake resolves it
```

**How it works:** Each JDBC connection maps to a persistent Query Service session. When you execute `USE SCHEMA`, it takes effect in the server-side Snowflake session and persists across all subsequent queries on that connection — just like a native Snowflake session.

You can also set the schema via the JDBC API:

```java
conn.setSchema("in.c-main");
// All subsequent queries use this schema
ResultSet rs = stmt.executeQuery("SELECT * FROM \"items_catalog\" LIMIT 10");
```

Or via connection properties:

```java
props.setProperty("schema", "in.c-main");
Connection conn = DriverManager.getConnection(url, props);
```

## Architecture

```
+-------------------------------------------+
|            JDBC Client                    |
|      (DBeaver / DataGrip / App)           |
+------------------+------------------------+
                   | JDBC API
+------------------v------------------------+
|         Keboola JDBC Driver               |
|                                           |
|  KeboolaDriver --> KeboolaConnection      |
|                    +-- KeboolaStatement    |
|                    +-- KeboolaResultSet    |
|                    +-- DatabaseMetaData    |
|                                           |
|  Command Dispatcher:                      |
|  +-- HelpCommandHandler                   |
|  +-- BackendSwitchHandler                 |
|  +-- PullCommandHandler                   |
|  +-- PushCommandHandler                   |
|  +-- SessionLogHandler                    |
|  +-- VirtualTableHandler --> Registry     |
|                                           |
|  Backends (switchable at runtime):        |
|  +-- QueryServiceBackend (cloud)          |
|  |   +-- QueryServiceClient (async HTTP)  |
|  +-- DuckDbBackend (local, embedded)      |
|      +-- DuckDB JDBC (nested JAR)         |
|                                           |
|  Session Logging:                         |
|  +-- SqlSessionLogger --> DuckDB table    |
|                                           |
|  HTTP Clients:                            |
|  +-- StorageApiClient (virtual tables)    |
|  +-- QueryServiceClient (SQL + metadata)  |
|  +-- JobQueueClient (job history)         |
+------+----------+----------+-------------+
       |          |          |
       v          v          v
  Storage API  Query Svc  Job Queue API
  (virt.tables) (SQL+meta) (job history)
```

**Query Service API** (`query.keboola.com`, auto-discovered): SQL execution and all sidebar metadata via Snowflake `SHOW` commands (`SHOW DATABASES`, `SHOW SCHEMAS`, `SHOW TABLES`, `SHOW COLUMNS`).

**Storage API** (`connection.keboola.com`): Token verification, branch/workspace discovery, and data for virtual `_keboola` tables (components, events, buckets, tables).

**Job Queue API** (`queue.keboola.com`, auto-discovered): Job history via `/search/jobs` endpoint. Lazy-initialized on first `_keboola.jobs` query.

**DuckDB** (embedded): Local SQL engine loaded from a nested JAR resource via `URLClassLoader`. Supports persistent file storage or in-memory ephemeral mode.

### Key Design Decisions

- **Snowflake-native metadata**: Sidebar shows real Snowflake objects via `SHOW` commands through the Query Service — no Storage API metadata invention
- **Async execution**: Submit job > poll with exponential backoff (100ms to 2s) > fetch paginated results
- **Command dispatcher**: Intercepts virtual table queries and KEBOOLA commands before they reach the backend
- **Virtual table isolation**: `_keboola` schema is injected into metadata results but excluded from Snowflake `SHOW` commands to avoid "object does not exist" errors
- **Uber-jar**: OkHttp, Jackson, and Kotlin runtime relocated to avoid classpath conflicts
- **Type mapping**: Snowflake types mapped to `java.sql.Types` (VARCHAR, NUMBER, BOOLEAN, DATE, TIMESTAMP, etc.)
- **Nested JAR for DuckDB**: DuckDB's JAR is embedded as a resource (not merged into the uber-jar) and loaded via `URLClassLoader` at runtime. This preserves the intact ZIP structure that DuckDB's native library loader requires — avoids `ZipException` in DBeaver/DataGrip classloader contexts
- **Backend abstraction**: `QueryBackend` interface with two implementations (`QueryServiceBackend`, `DuckDbBackend`) enables runtime switching via Strategy pattern
- **Session logging**: All SQL captured in a DuckDB system table for dialect compatibility analysis between local and cloud execution

## Running Tests

```bash
mvn test      # 235 unit tests
make test     # same thing via Makefile
```

Unit tests cover: `TypeMapper`, `ConnectionConfig`, `ArrayResultSet`, `KeboolaDriver`, `SchemaCache`, `CommandDispatcher`, `HelpCommandHandler`, `VirtualTableHandler`, `BackendSwitchHandler`, `PullCommandHandler`, `PushCommandHandler`, `SessionLogHandler`, `SqlSessionLogger`, `DuckDbBackend`, `QueryServiceBackend`, `KeboolaStatement`.

### Integration Tests

```bash
export KEBOOLA_TOKEN="your-token"
export KEBOOLA_WORKSPACE="your-workspace-id"  # recommended
make test-e2e                                   # Query Service E2E tests
```

### DuckDB Integration Tests

```bash
mvn verify -Pduckdb-integration               # DuckDB + backend switching E2E tests (no cloud needed)
```

### Manual Integration Test

```bash
export KEBOOLA_TOKEN="your-token"
export KEBOOLA_WORKSPACE="your-workspace-id"    # recommended
export KEBOOLA_HOST="connection.keboola.com"    # optional
make manual-test
```

## Project Structure

```
src/main/java/com/keboola/jdbc/
+-- KeboolaDriver.java                # SPI entry point, URL parsing
+-- KeboolaConnection.java            # Connection lifecycle, service discovery, backend management
+-- KeboolaStatement.java             # SQL execution, command interception, session logging
+-- KeboolaPreparedStatement.java     # Parameterized queries
+-- KeboolaResultSet.java             # Lazy-paging result set (Query Service)
+-- KeboolaDatabaseMetaData.java      # SHOW commands metadata (+ virtual _keboola)
+-- ArrayResultSet.java               # In-memory ResultSet for metadata
+-- backend/
|   +-- QueryBackend.java             # Backend interface (Strategy pattern)
|   +-- ExecutionResult.java          # Result value object
|   +-- QueryServiceBackend.java      # Keboola Query Service (async HTTP)
|   +-- DuckDbBackend.java            # Embedded DuckDB (local JDBC via nested JAR)
+-- command/
|   +-- KeboolaCommandHandler.java    # Handler interface
|   +-- KeboolaCommandDispatcher.java # Chain-of-responsibility dispatcher
|   +-- HelpCommandHandler.java       # KEBOOLA HELP command
|   +-- BackendSwitchHandler.java     # KEBOOLA USE BACKEND command
|   +-- PullCommandHandler.java       # KEBOOLA PULL TABLE/QUERY command
|   +-- PushCommandHandler.java       # KEBOOLA PUSH TABLE command
|   +-- SessionLogHandler.java        # KEBOOLA SESSION LOG command
|   +-- VirtualTableHandler.java      # _keboola.* SQL detection + LIMIT parsing
|   +-- VirtualTableRegistry.java     # API calls --> ArrayResultSet for each table
|   +-- VirtualTableMetadata.java     # Column definitions for IDE integration
+-- config/
|   +-- DriverConfig.java             # Driver constants and defaults
|   +-- ConnectionConfig.java         # URL + properties parsing
+-- http/
|   +-- StorageApiClient.java         # Storage API v2 (virtual tables + discovery)
|   +-- QueryServiceClient.java       # Query Service API v1 (SQL + SHOW metadata)
|   +-- JobQueueClient.java           # Job Queue API client (lazy init)
|   +-- model/                        # API data models
+-- logging/
|   +-- SqlSessionLogger.java         # SQL session logging to DuckDB table
+-- meta/
|   +-- TypeMapper.java               # Snowflake --> JDBC type mapping
+-- exception/
    +-- KeboolaJdbcException.java     # SQLSTATE error codes
```

## Contributors

- **David Esner** ([@davidesner](https://github.com/davidesner)) — architect of the Snowflake-native metadata layer (2.0.0). Replaced Storage API metadata with `SHOW` commands via Query Service, `initCatalogAndSchema()`, and server-side `USE DATABASE`/`USE SCHEMA`. See [PR #2](https://github.com/padak/keboola_jdbc/pull/2).
- **Jan Botorek** ([@jbotor](https://github.com/jbotor)) — found and fixed `StackOverflowError` recursion in `setSchema()`/`interceptUseCommand()` loop (2.1.0). See [PR #4](https://github.com/padak/keboola_jdbc/pull/4).

## Changelog

### 3.0.1-experimental

- **DuckDB local backend**: Embedded DuckDB as a local SQL engine. Connect with `backend=duckdb` for zero-cloud SQL development. Supports persistent file storage (`duckdbPath`) and in-memory ephemeral mode.
- **Runtime backend switching**: `KEBOOLA USE BACKEND duckdb` / `KEBOOLA USE BACKEND queryservice` switches between local and cloud execution at runtime. Both backends can be initialized simultaneously when a token is provided.
- **PULL command**: `KEBOOLA PULL TABLE <source> INTO <local_table>` and `KEBOOLA PULL QUERY <sql> INTO <local_table>` transfer data from cloud to local DuckDB.
- **PUSH command**: `KEBOOLA PUSH TABLE <local_table> [INTO <target>]` transfers data from local DuckDB to cloud (Snowflake via Query Service). Creates VARCHAR tables, inserts in batches.
- **Session logging**: All SQL execution is logged to `_keboola_session_log` DuckDB table with timing, backend, success/error, and row counts. View with `KEBOOLA SESSION LOG`.
- **Nested JAR for DuckDB**: DuckDB JAR embedded as a resource and loaded via `URLClassLoader` to avoid `ZipException` in DBeaver/DataGrip classloader contexts.
- **QueryBackend interface**: Strategy pattern abstraction (`QueryServiceBackend`, `DuckDbBackend`) enables clean backend switching.
- **BackendSwitchHandler regex fix**: Handles trailing semicolons from DBeaver SQL editor.

### 3.0.0-experimental

- **Backend abstraction**: Introduced `QueryBackend` interface with `QueryServiceBackend` and `DuckDbBackend` implementations. Pure refactor of SQL execution path — no behavior change for existing Query Service users.
- **DuckDB backend (initial)**: First implementation of embedded DuckDB backend with in-memory and file-based modes.

### 2.1.0

- **Fix StackOverflow in setSchema/interceptUseCommand**: `setSchema()` called `execute("USE SCHEMA ...")` which triggered `interceptUseCommand()` which called `setSchema()` again — infinite recursion. Fixed by introducing `updateLocalSchema()` that only updates the local field. Credit: [@jbotor](https://github.com/jbotor) ([PR #4](https://github.com/padak/keboola_jdbc/pull/4)).

### 2.0.2

- **Enhanced `_keboola.components`**: Now shows one row per configuration instead of one row per component. Includes `config_id`, `config_name`, `config_description`, `version`, `created`, `created_by`, `is_disabled`, `is_deleted`. Joinable with `_keboola.jobs` via `component_id` + `config_id`.

### 2.0.1

- **Filter system databases**: `SNOWFLAKE` and `SNOWFLAKE_LEARNING_DB` databases are hidden from the sidebar. These Snowflake system databases are not part of the Keboola project and were cluttering the database navigator.

### 2.0.0

- **BREAKING: Snowflake-native metadata**: Sidebar now shows real Snowflake databases, schemas, tables, and views via `SHOW` commands through the Query Service. Replaces the previous Storage API-based metadata layer. Catalogs are now Snowflake databases (not Keboola projects), schemas are Snowflake schemas (not buckets).
- **Connection init discovers server state**: `initCatalogAndSchema()` runs `SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()` at connect time to sync the driver with the Snowflake session.
- **`setCatalog()` / `setSchema()` execute server commands**: `USE DATABASE` and `USE SCHEMA` are sent to the Query Service, affecting the real Snowflake session.
- **Virtual tables preserved**: `_keboola` schema with 5 virtual tables still works alongside real Snowflake metadata. Virtual tables are isolated from `SHOW` commands to prevent "object does not exist" errors.
- **Removed Storage API metadata dependency**: `SchemaCache` and Storage API bucket/table/column listing no longer used for the sidebar. Storage API is still used for token verification, service discovery, and virtual table data.

### 1.4.0-experimental

- **Virtual tables**: New `_keboola` schema with 5 virtual tables (`components`, `events`, `jobs`, `tables`, `buckets`) backed by Keboola APIs — instant results, no Snowflake queries. Supports `LIMIT` clause.
- **KEBOOLA HELP command**: `KEBOOLA HELP;` returns a table of available virtual tables and commands.
- **Command dispatcher**: Chain-of-responsibility pattern intercepts SQL before it reaches the Query Service. Extensible for future custom commands.
- **IDE sidebar integration**: `_keboola` appears as a schema in DBeaver/DataGrip with tables and column autocomplete. "View Data" works from the sidebar context menu.
- **Job Queue API client**: New `JobQueueClient` with lazy initialization for `_keboola.jobs` queries via `/search/jobs` endpoint.

### 1.3.0

- **Server-side session persistence**: Each JDBC connection now maps to a persistent Query Service session (via `sessionId`). `SET` variables, `USE SCHEMA`, and temporary tables persist across separate `execute()` calls — just like a native Snowflake connection.
- **Multi-statement SQL support**: Semicolon-separated statements (e.g. `SET X=1; SELECT $X`) are split and submitted as a single job, respecting quoted strings.

### 1.2.0

- **USE SCHEMA support via multi-statement jobs**: `USE SCHEMA` / `Connection.setSchema()` / `schema` connection property now work correctly. The driver prepends a `USE SCHEMA` statement to each query job so both execute in the same Snowflake session. Removed the previous regex-based SQL rewriting approach.
- **New `schema` connection property**: Set default schema at connect time via `props.setProperty("schema", "in.c-main")`.

### 1.1.0

- Initial release with Storage API metadata, Query Service SQL execution, DBeaver/DataGrip support, type mapping, schema cache, and uber-jar packaging.
