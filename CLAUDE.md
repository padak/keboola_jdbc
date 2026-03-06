# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Keboola JDBC Driver - connects JDBC clients (DBeaver, DataGrip) to Keboola projects. Two API layers:
- **Storage API** (`connection.keboola.com`) - connection setup: tokens, branches, workspaces. Also provides data for virtual `_keboola.*` tables.
- **Query Service API** (`query.keboola.com`, auto-discovered) - async SQL execution against Snowflake via workspace

Supports an embedded **DuckDB local backend** for offline SQL development with runtime backend switching.

JDBC mapping: Catalog = Database (from Snowflake), Schema = Schema (from Snowflake), Table = Table. All metadata (databases, schemas, tables, columns) comes from SHOW commands executed via the Query Service (or native DuckDB metadata when using DuckDB backend). Virtual `_keboola.*` tables provide Keboola platform metadata (jobs, components, events, etc.) from the Storage API.

## Build & Test Commands

```bash
mvn clean package          # Build uber-jar (target/keboola-jdbc-driver-3.0.2-experimental.jar)
mvn test                   # Run all 235 unit tests
mvn test -pl . -Dtest=TypeMapperTest          # Run single test class
mvn test -pl . -Dtest=TypeMapperTest#testVarchar  # Run single test method
mvn verify -Pkeboola-integration              # Run Query Service integration tests (needs KEBOOLA_TOKEN env)
mvn verify -Pduckdb-integration               # Run DuckDB + backend switching integration tests (no token needed)
make dist                  # Build uber-jar and copy to dist/
```

Manual integration test:
```bash
KEBOOLA_TOKEN=xxx java -cp target/keboola-jdbc-driver-3.0.2-experimental.jar com.keboola.jdbc.ManualConnectionTest
```

**After every version bump or code change, always run `make dist`** to rebuild the uber-jar and copy it to `dist/`. The `dist/` directory contains the release-ready jars that users download. **Never delete old jars from `dist/`** — keep all previous versions for version history.

Java 11+. Surefire needs `-Dnet.bytebuddy.experimental=true` (already configured in pom.xml for Java 25 compat).

## Architecture

### QueryBackend Abstraction
`QueryBackend` interface abstracts SQL execution. Two implementations:
- **`QueryServiceBackend`** — wraps `QueryServiceClient` (async HTTP API: submit->poll->fetch). Backend engine (Snowflake, etc.) transparent.
- **`DuckDbBackend`** — wraps embedded DuckDB JDBC (synchronous, local). Zero cloud dependency. DuckDB JAR embedded as nested resource, loaded via `URLClassLoader` to avoid ZipException in IDE classloader contexts.

Runtime switching via `KEBOOLA USE BACKEND duckdb|queryservice` command.

### Commands
- `KEBOOLA HELP` — list available commands and virtual tables
- `KEBOOLA USE BACKEND duckdb|queryservice` — switch active backend at runtime
- `KEBOOLA PULL TABLE <source> INTO <local_table>` — pull cloud table into local DuckDB
- `KEBOOLA PULL QUERY <sql> INTO <local_table>` — pull query result into local DuckDB
- `KEBOOLA PUSH TABLE <local_table> [INTO <target>]` — push local DuckDB table to cloud (Snowflake)
- `KEBOOLA SESSION LOG` — view SQL session log from DuckDB system table
- `KEBOOLA KAI ASK <question>` — ask Kai AI assistant a free-form question
- `KEBOOLA KAI SQL <description>` — ask Kai to generate SQL for current backend
- `KEBOOLA KAI HELP <log_id>` — ask Kai to help fix a query from session log
- `KEBOOLA KAI TRANSLATE TO SNOWFLAKE|DUCKDB <sql>` — translate SQL to target dialect
- `KEBOOLA KAI TRANSLATE <log_id>` — translate session log query to the other dialect

### SQL Execution Flow (Query Service)
`KeboolaStatement.execute(sql)` -> `QueryServiceBackend.execute()` -> `QueryServiceClient.submitJob()` -> poll `waitForCompletion()` with exponential backoff (100ms->2s) -> `fetchResults()` (paginated, 1000 rows/page) -> `KeboolaResultSet` (lazy paging)

### SQL Execution Flow (DuckDB)
`KeboolaStatement.execute(sql)` -> `DuckDbBackend.execute()` -> DuckDB native `Statement.execute()` -> native `ResultSet`

### Connection Setup Flow
`KeboolaConnection(config)` -> if token: `StorageApiClient.verifyToken()` -> `discoverQueryServiceUrl()` -> `resolveBranchId()` -> `resolveWorkspaceId()` -> create `QueryServiceBackend`; if backend=duckdb: create `DuckDbBackend` -> `initCatalogAndSchema()` via active backend

### Key Patterns
- **Metadata via SHOW commands or native**: For Query Service, `KeboolaDatabaseMetaData` uses SHOW commands. For DuckDB, delegates to native `DatabaseMetaData`. Branch determined by `backend.getNativeMetaData()` returning null (SHOW) or non-null (native).
- **USE SCHEMA/DATABASE interception**: `setCatalog()`/`setSchema()` execute `USE DATABASE`/`USE SCHEMA` commands on the server. Driver also intercepts USE statements in SQL.
- **Virtual tables (`_keboola.*`)**: `VirtualTableMetadata` defines schema for `_keboola.components`, `_keboola.jobs`, `_keboola.events`, `_keboola.tables`, `_keboola.buckets`. Handled by `VirtualTableHandler` via `KeboolaCommandDispatcher`.
- **Session logging**: `SqlSessionLogger` logs all SQL execution to `_keboola_session_log` DuckDB table with timing, backend, success/error, and row counts.
- **All constants in `DriverConfig`**: Page sizes, timeouts, polling intervals, retry counts, batch sizes. Never hardcode values elsewhere.
- **`ArrayResultSet`**: In-memory ResultSet used by `KeboolaDatabaseMetaData` for metadata queries (getSchemas, getTables, getColumns, etc.).
- **Uber-jar via maven-shade-plugin**: OkHttp, Jackson, Kotlin runtime relocated under `com.keboola.jdbc.shaded.*`. DuckDB JAR embedded as nested resource (not merged) — loaded via `URLClassLoader` at runtime to preserve ZIP integrity for native lib extraction.
- **SPI registration**: `META-INF/services/java.sql.Driver` -> `com.keboola.jdbc.KeboolaDriver`

### API Specifics
- Workspace IDs can exceed `Integer.MAX_VALUE` — use `long` everywhere (not `int`)
- Storage API branches URL: `/v2/storage/dev-branches` (no trailing slash, follow redirects)
- Query Service result page size minimum is 100
- `StatementStatus.rowsAffected` is 0 for SELECT too — use `numberOfRows > 0` + SQL keyword heuristic to distinguish SELECT from DML

## Testing

- Unit tests (277): TypeMapperTest, ConnectionConfigTest, ConnectionConfigDuckDbTest, ArrayResultSetTest, KeboolaDriverTest, KeboolaStatementTest, SchemaCacheTest, DuckDbBackendTest, QueryServiceBackendTest, BackendSwitchHandlerTest, PullCommandHandlerTest, PushCommandHandlerTest, SessionLogHandlerTest, SqlSessionLoggerTest, HelpCommandHandlerTest, VirtualTableHandlerTest, KeboolaCommandDispatcherTest, KaiClientTest, KaiResponseTest, KaiCommandHandlerTest
- Integration tests: `DuckDbDriverIT` (no token needed), `BackendSwitchIT` (no token needed), `PullCommandIT` (needs KEBOOLA_TOKEN), `PushCommandIT` (needs KEBOOLA_TOKEN), `KeboolaDriverIT` (needs KEBOOLA_TOKEN)
- `ManualConnectionTest` is a CLI integration test (not run by `mvn test`), needs `KEBOOLA_TOKEN` env var
- Use JUnit 5 + Mockito 5.11 + DuckDB JDBC 1.1.3
