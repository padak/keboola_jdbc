# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Keboola JDBC Driver - connects JDBC clients (DBeaver, DataGrip) to Keboola projects. Two API layers:
- **Storage API** (`connection.keboola.com`) - metadata: tokens, branches, workspaces, buckets, tables, columns
- **Query Service API** (`query.keboola.com`, auto-discovered) - async SQL execution against Snowflake via workspace

JDBC mapping: Catalog = Project, Schema = Bucket (`in.c-main`), Table = Table.

## Build & Test Commands

```bash
mvn clean package          # Build uber-jar (target/keboola-jdbc-driver-1.2.0.jar)
mvn test                   # Run all unit tests (126 tests)
mvn test -pl . -Dtest=TypeMapperTest          # Run single test class
mvn test -pl . -Dtest=TypeMapperTest#testVarchar  # Run single test method
mvn verify -Pkeboola-integration              # Run integration tests (needs KEBOOLA_TOKEN env)
```

Manual integration test:
```bash
KEBOOLA_TOKEN=xxx java -cp target/keboola-jdbc-driver-1.2.0.jar com.keboola.jdbc.ManualConnectionTest
```

**After every version bump or code change, always run `make dist`** to rebuild the uber-jar and copy it to `dist/`. The `dist/` directory contains the release-ready jars that users download. **Never delete old jars from `dist/`** — keep all previous versions for version history.

Java 11+. Surefire needs `-Dnet.bytebuddy.experimental=true` (already configured in pom.xml for Java 25 compat).

## Architecture

### SQL Execution Flow
`KeboolaStatement.execute(sql)` → `QueryServiceClient.submitJob()` → poll `waitForCompletion()` with exponential backoff (100ms→2s) → `fetchResults()` (paginated, 1000 rows/page) → `KeboolaResultSet` (lazy paging)

### Connection Setup Flow
`KeboolaConnection(config)` → `StorageApiClient.verifyToken()` → `discoverQueryServiceUrl()` (from Storage API index) → `resolveBranchId()` (auto-detect default) → `resolveWorkspaceId()` (auto-select newest) → create `QueryServiceClient`

### Key Patterns
- **USE SCHEMA via multi-statement jobs**: Driver intercepts `USE SCHEMA/DATABASE` SQL and stores schema in `KeboolaConnection.currentSchema`. When a schema is set, `KeboolaStatement.buildStatements()` prepends `USE SCHEMA "xxx"` to the statements array so both execute in the same Snowflake session. The database itself handles schema resolution — no SQL rewriting in the driver. Connection property `schema` sets the default schema at connect time.
- **All constants in `DriverConfig`**: Page sizes, timeouts, polling intervals, retry counts. Never hardcode values elsewhere.
- **`SchemaCache`**: 60s TTL with stale-on-error fallback. Caches buckets and tables-per-bucket from Storage API.
- **`ArrayResultSet`**: In-memory ResultSet used by `KeboolaDatabaseMetaData` for metadata queries (getSchemas, getTables, getColumns, etc.).
- **Uber-jar via maven-shade-plugin**: OkHttp, Jackson, Kotlin runtime relocated under `com.keboola.jdbc.shaded.*` to avoid classpath conflicts with host applications.
- **SPI registration**: `META-INF/services/java.sql.Driver` → `com.keboola.jdbc.KeboolaDriver`

### API Specifics
- Workspace IDs can exceed `Integer.MAX_VALUE` — use `long` everywhere (not `int`)
- Storage API branches URL: `/v2/storage/dev-branches` (no trailing slash, follow redirects)
- Query Service result page size minimum is 100
- `StatementStatus.rowsAffected` is 0 for SELECT too — use `numberOfRows > 0` + SQL keyword heuristic to distinguish SELECT from DML

## Testing

- Unit tests in `src/test/java/com/keboola/jdbc/`: TypeMapperTest, ConnectionConfigTest, ArrayResultSetTest, KeboolaDriverTest, SchemaCacheTest
- `ManualConnectionTest` is a CLI integration test (not run by `mvn test`), needs `KEBOOLA_TOKEN` env var
- Use JUnit 5 + Mockito 5.11
