# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Keboola JDBC Driver - connects JDBC clients (DBeaver, DataGrip) to Keboola projects. Two API layers:
- **Storage API** (`connection.keboola.com`) - connection setup: tokens, branches, workspaces. Also provides data for virtual `_keboola.*` tables.
- **Query Service API** (`query.keboola.com`, auto-discovered) - async SQL execution against Snowflake via workspace

JDBC mapping: Catalog = Database (from Snowflake), Schema = Schema (from Snowflake), Table = Table. All metadata (databases, schemas, tables, columns) comes from SHOW commands executed via the Query Service. Virtual `_keboola.*` tables provide Keboola platform metadata (jobs, components, events, etc.) from the Storage API.

## Build & Test Commands

```bash
mvn clean package          # Build uber-jar (target/keboola-jdbc-driver-2.1.2.jar)
mvn test                   # Run all unit tests
mvn test -pl . -Dtest=TypeMapperTest          # Run single test class
mvn test -pl . -Dtest=TypeMapperTest#testVarchar  # Run single test method
mvn verify -Pkeboola-integration              # Run integration tests (needs KEBOOLA_TOKEN env)
```

Manual integration test:
```bash
KEBOOLA_TOKEN=xxx java -cp target/keboola-jdbc-driver-2.1.2.jar com.keboola.jdbc.ManualConnectionTest
```

**After every version bump or code change, always run `make dist`** to rebuild the uber-jar and copy it to `dist/`. The `dist/` directory contains the release-ready jars that users download. **Never delete old jars from `dist/`** — keep all previous versions for version history.

Java 11+. Surefire needs `-Dnet.bytebuddy.experimental=true` (already configured in pom.xml for Java 25 compat).

## Release Checklist

**MANDATORY: Before merging to main and creating a release, ALWAYS run E2E integration tests first.** Do NOT commit, tag, or release until E2E tests pass. The correct order is:

1. Run unit tests: `mvn test`
2. Run E2E tests: `KEBOOLA_TOKEN=xxx KEBOOLA_WORKSPACE=yyy mvn verify -Pkeboola-integration`
3. Fix any failures
4. Only then: bump version, `make dist`, commit, tag, `gh release create`

This prevents releasing broken drivers. Unit tests alone are not sufficient — E2E tests catch real API behavior differences (e.g. backendContext not returning schema for certain queries).

## Architecture

### SQL Execution Flow
`KeboolaStatement.execute(sql)` → `QueryServiceClient.submitJob()` → poll `waitForCompletion()` with exponential backoff (100ms→2s) → `fetchResults()` (paginated, 1000 rows/page) → `KeboolaResultSet` (lazy paging)

### Connection Setup Flow
`KeboolaConnection(config)` → `StorageApiClient.verifyToken()` → `discoverQueryServiceUrl()` (from Storage API index) → `resolveBranchId()` (auto-detect default) → `resolveWorkspaceId()` (auto-select newest) → create `QueryServiceClient` → `initCatalogAndSchema()` (executes `SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()`)

### Key Patterns
- **Metadata via SHOW commands**: `KeboolaDatabaseMetaData` executes `SHOW DATABASES`, `SHOW SCHEMAS`, `SHOW TABLES/VIEWS/OBJECTS`, and `SHOW COLUMNS` via the connection to proxy real Snowflake metadata. Column type info is parsed from JSON `data_type` field in `SHOW COLUMNS` output. Virtual `_keboola` schema is injected alongside real Snowflake schemas.
- **Schema/catalog tracking via backendContext**: After each query, the driver reads `backendContext.catalog` and `backendContext.schema` from the Query Service response to keep local state in sync. Falls back to parsing USE SCHEMA/DATABASE from SQL if backendContext is not available. `setCatalog()`/`setSchema()` execute `USE DATABASE`/`USE SCHEMA` on the server.
- **Virtual tables (`_keboola.*`)**: `VirtualTableMetadata` defines schema for `_keboola.components`, `_keboola.jobs`, `_keboola.events`, `_keboola.tables`, `_keboola.buckets`. Handled by `VirtualTableHandler` via `KeboolaCommandDispatcher`.
- **All constants in `DriverConfig`**: Page sizes, timeouts, polling intervals, retry counts. Never hardcode values elsewhere.
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
