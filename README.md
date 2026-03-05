# Keboola JDBC Driver

JDBC driver for [Keboola](https://www.keboola.com/) that connects any JDBC-compatible client (DBeaver, DataGrip, etc.) to Keboola projects via the Storage API and Query Service API.

User provides only an **API token and stack URL** — the driver auto-discovers branches, workspaces, and metadata.

## JDBC Mapping

| JDBC Concept | Keboola Concept | Example |
|---|---|---|
| Catalog | Project | `Padak` |
| Schema | Bucket | `in.c-main` |
| Table | Table | `data` |

## Quick Start

### Build

```bash
mvn clean package
```

Produces an uber-jar at `target/keboola-jdbc-driver-1.4.0-experimental.jar` (~10 MB, all dependencies shaded).

Requires **Java 11+**.

### Connection Properties

| Property | Required | Description |
|---|---|---|
| `token` | Yes | Keboola Storage API token |
| `branch` | No | Branch ID (auto-detects default branch) |
| `workspace` | No | Workspace ID (auto-selects newest available) |
| `schema` | No | Default schema/bucket (e.g. `in.c-main`) for unqualified table refs |

### JDBC URL Format

```
jdbc:keboola://<host>
```

Examples:
```
jdbc:keboola://connection.keboola.com
jdbc:keboola://connection.north-europe.azure.keboola.com
```

### Java Example

```java
String url = "jdbc:keboola://connection.keboola.com";
Properties props = new Properties();
props.setProperty("token", System.getenv("KEBOOLA_TOKEN"));

try (Connection conn = DriverManager.getConnection(url, props)) {
    // Browse schemas (buckets)
    try (ResultSet rs = conn.getMetaData().getSchemas()) {
        while (rs.next()) {
            System.out.println(rs.getString("TABLE_SCHEM"));
        }
    }

    // Execute SQL
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT * FROM \"in.c-main\".\"items_catalog\" LIMIT 10")) {
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
}
```

## DBeaver Setup

1. **Database** > **Driver Manager** > **New**
2. Set **Driver Name** to `Keboola`
3. **Libraries** tab > **Add File** > select `target/keboola-jdbc-driver-1.4.0-experimental.jar`
4. Set **Class Name** to `com.keboola.jdbc.KeboolaDriver`
5. Set **URL Template** to `jdbc:keboola://connection.keboola.com`
6. **OK** > **New Database Connection** > select `Keboola`
7. In **Driver Properties**, set `token` to your Keboola API token
8. **Test Connection**

The database navigator will show your buckets as schemas and tables with columns.

## Virtual Tables (Experimental)

The driver exposes Keboola platform metadata as virtual SQL tables in the `_keboola` schema. These tables are backed by Keboola APIs — no Snowflake queries involved, instant results.

### Available virtual tables

| Virtual table | Description |
|---|---|
| `_keboola.components` | Configured components with config counts |
| `_keboola.events` | Recent storage events (supports `LIMIT`) |
| `_keboola.jobs` | Recent jobs with status and duration (supports `LIMIT`) |
| `_keboola.tables` | All tables with row counts and sizes |
| `_keboola.buckets` | All buckets with metadata |

```sql
-- See what's available
KEBOOLA HELP;

-- Browse configured components
SELECT * FROM _keboola.components;

-- Last 10 storage events
SELECT * FROM _keboola.events LIMIT 10;

-- Recent job runs with status
SELECT * FROM _keboola.jobs LIMIT 20;

-- All tables with sizes
SELECT * FROM _keboola.tables;
```

### IDE integration

The `_keboola` schema appears in the DBeaver/DataGrip sidebar alongside real buckets:

```
Padak (project)
  _keboola          <-- virtual schema
    Tables
      buckets
      components
      events
      jobs
      tables
  in.c-main         <-- real Snowflake buckets
  out.c-results
```

Autocomplete works for virtual table and column names. "View Data" from the sidebar context menu works too.

### How it works

The command dispatcher intercepts SQL in `KeboolaStatement.execute()` *before* it reaches the Query Service. If the SQL matches a virtual table pattern (`_keboola.*`) or a command (`KEBOOLA HELP`), it returns an in-memory `ArrayResultSet` directly — zero network calls to the Query Service.

```
SQL input
  │
  ├─ "KEBOOLA HELP"           → HelpCommandHandler → ArrayResultSet
  ├─ "_keboola.components"    → VirtualTableHandler → Storage API → ArrayResultSet
  ├─ "_keboola.jobs LIMIT 5"  → VirtualTableHandler → Job Queue API → ArrayResultSet
  └─ "SELECT * FROM ..."      → Query Service (normal flow)
```

## USE SCHEMA Support

The driver supports setting a default schema so you can use unqualified table names:

```sql
USE SCHEMA "in.c-main";
SELECT * FROM "items_catalog" LIMIT 10;
-- No need to write "in.c-main"."items_catalog" — Snowflake resolves it
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
┌─────────────────────────────────────────┐
│              JDBC Client                │
│        (DBeaver / DataGrip / App)       │
└──────────────┬──────────────────────────┘
               │ JDBC API
┌──────────────▼──────────────────────────┐
│         Keboola JDBC Driver             │
│                                         │
│  KeboolaDriver ─► KeboolaConnection     │
│                    ├─ KeboolaStatement  │
│                    ├─ KeboolaResultSet  │
│                    └─ DatabaseMetaData  │
│                                         │
│  Command Dispatcher:                    │
│  ├─ HelpCommandHandler                  │
│  └─ VirtualTableHandler → Registry      │
│                                         │
│  HTTP Clients:                          │
│  ├─ StorageApiClient (metadata)         │
│  ├─ QueryServiceClient (SQL execution)  │
│  └─ JobQueueClient (job history)        │
└──────┬──────────┬──────────┬────────────┘
       │          │          │
       ▼          ▼          ▼
  Storage API  Query Svc  Job Queue API
  (metadata)   (SQL)      (job history)
```

**Storage API** (`connection.keboola.com`): Token verification, branch/workspace discovery, bucket/table/column metadata, components, events.

**Query Service API** (`query.keboola.com`, auto-discovered): Async SQL execution with job polling and paginated results.

**Job Queue API** (`queue.keboola.com`, auto-discovered): Job history via `/search/jobs` endpoint. Lazy-initialized on first `_keboola.jobs` query.

### Key Design Decisions

- **Async execution**: Submit job > poll with exponential backoff (100ms to 2s) > fetch paginated results
- **Command dispatcher**: Intercepts virtual table queries and KEBOOLA commands before they reach Query Service
- **Schema cache**: 60s TTL, stale-on-error fallback
- **Uber-jar**: OkHttp, Jackson, and Kotlin runtime relocated to avoid classpath conflicts
- **Type mapping**: Snowflake types mapped to `java.sql.Types` (VARCHAR, NUMBER, BOOLEAN, DATE, TIMESTAMP, etc.)

## Running Tests

```bash
mvn test
```

Unit tests cover: `TypeMapper`, `ConnectionConfig`, `ArrayResultSet`, `KeboolaDriver`, `SchemaCache`, `CommandDispatcher`, `HelpCommandHandler`, `VirtualTableHandler` (172 tests).

### Integration Tests

```bash
export KEBOOLA_TOKEN="your-token"
export KEBOOLA_WORKSPACE="your-workspace-id"  # recommended
make test-e2e                                   # 32 E2E tests
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
├── KeboolaDriver.java            # SPI entry point, URL parsing
├── KeboolaConnection.java        # Connection lifecycle, service discovery
├── KeboolaStatement.java         # SQL execution, command interception
├── KeboolaPreparedStatement.java # Parameterized queries
├── KeboolaResultSet.java         # Lazy-paging result set
├── KeboolaDatabaseMetaData.java  # Schema browser (+ virtual _keboola schema)
├── ArrayResultSet.java           # In-memory ResultSet for metadata
├── command/
│   ├── KeboolaCommandHandler.java    # Handler interface
│   ├── KeboolaCommandDispatcher.java # Chain-of-responsibility dispatcher
│   ├── HelpCommandHandler.java       # KEBOOLA HELP command
│   ├── VirtualTableHandler.java      # _keboola.* SQL detection + LIMIT parsing
│   ├── VirtualTableRegistry.java     # API calls → ArrayResultSet for each table
│   └── VirtualTableMetadata.java     # Column definitions for IDE integration
├── config/
│   ├── DriverConfig.java         # Driver constants and defaults
│   └── ConnectionConfig.java     # URL + properties parsing
├── http/
│   ├── StorageApiClient.java     # Storage API v2 client
│   ├── QueryServiceClient.java   # Query Service API v1 client
│   ├── JobQueueClient.java       # Job Queue API client (lazy init)
│   └── model/                    # API data models
├── meta/
│   ├── TypeMapper.java           # Snowflake → JDBC type mapping
│   └── SchemaCache.java          # Metadata cache with TTL
└── exception/
    └── KeboolaJdbcException.java # SQLSTATE error codes
```

## Changelog

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
