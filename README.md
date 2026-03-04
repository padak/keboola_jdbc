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

Produces an uber-jar at `target/keboola-jdbc-driver-1.1.0.jar` (~10 MB, all dependencies shaded).

Requires **Java 11+**.

### Connection Properties

| Property | Required | Description |
|---|---|---|
| `token` | Yes | Keboola Storage API token |
| `branch` | No | Branch ID (auto-detects default branch) |
| `workspace` | No | Workspace ID (auto-selects newest available) |

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
             "SELECT * FROM \"in.c-gymbeam\".\"items_catalog\" LIMIT 10")) {
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
}
```

## DBeaver Setup

1. **Database** > **Driver Manager** > **New**
2. Set **Driver Name** to `Keboola`
3. **Libraries** tab > **Add File** > select `target/keboola-jdbc-driver-1.1.0.jar`
4. Set **Class Name** to `com.keboola.jdbc.KeboolaDriver`
5. Set **URL Template** to `jdbc:keboola://connection.keboola.com`
6. **OK** > **New Database Connection** > select `Keboola`
7. In **Driver Properties**, set `token` to your Keboola API token
8. **Test Connection**

The database navigator will show your buckets as schemas and tables with columns.

## USE SCHEMA Support

The Query Service API is stateless and does not support `USE SCHEMA` natively. The driver handles this client-side:

```sql
USE SCHEMA "in.c-gymbeam";
SELECT * FROM "items_catalog" LIMIT 10;
-- Driver rewrites to: SELECT * FROM "in.c-gymbeam"."items_catalog" LIMIT 10
```

The driver intercepts `USE SCHEMA` / `USE DATABASE` commands, stores the active schema, and automatically qualifies unqualified table references in subsequent queries.

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
│  HTTP Clients:                          │
│  ├─ StorageApiClient (metadata)         │
│  └─ QueryServiceClient (SQL execution)  │
└──────┬─────────────────┬────────────────┘
       │                 │
       ▼                 ▼
  Storage API      Query Service API
  (metadata)       (SQL execution)
```

**Storage API** (`connection.keboola.com`): Token verification, branch/workspace discovery, bucket/table/column metadata.

**Query Service API** (`query.keboola.com`, auto-discovered): Async SQL execution with job polling and paginated results.

### Key Design Decisions

- **Async execution**: Submit job > poll with exponential backoff (100ms to 2s) > fetch paginated results
- **Schema cache**: 60s TTL, stale-on-error fallback
- **Uber-jar**: OkHttp, Jackson, and Kotlin runtime relocated to avoid classpath conflicts
- **Type mapping**: Snowflake types mapped to `java.sql.Types` (VARCHAR, NUMBER, BOOLEAN, DATE, TIMESTAMP, etc.)

## Running Tests

```bash
mvn test
```

Unit tests cover: `TypeMapper`, `ConnectionConfig`, `ArrayResultSet`, `KeboolaDriver`, `SchemaCache` (123 tests).

### Manual Integration Test

```bash
export KEBOOLA_TOKEN="your-token"
export KEBOOLA_HOST="connection.keboola.com"  # optional
java -cp target/keboola-jdbc-driver-1.1.0.jar com.keboola.jdbc.ManualConnectionTest
```

## Project Structure

```
src/main/java/com/keboola/jdbc/
├── KeboolaDriver.java            # SPI entry point, URL parsing
├── KeboolaConnection.java        # Connection lifecycle, service discovery
├── KeboolaStatement.java         # SQL execution, USE SCHEMA interception
├── KeboolaPreparedStatement.java # Parameterized queries
├── KeboolaResultSet.java         # Lazy-paging result set
├── KeboolaDatabaseMetaData.java  # Schema browser (catalogs/schemas/tables/columns)
├── ArrayResultSet.java           # In-memory ResultSet for metadata
├── config/
│   ├── DriverConfig.java         # Driver constants and defaults
│   └── ConnectionConfig.java     # URL + properties parsing
├── http/
│   ├── StorageApiClient.java     # Storage API v2 client
│   ├── QueryServiceClient.java   # Query Service API v1 client
│   └── model/                    # API data models
├── meta/
│   ├── TypeMapper.java           # Snowflake → JDBC type mapping
│   └── SchemaCache.java          # Metadata cache with TTL
└── exception/
    └── KeboolaJdbcException.java # SQLSTATE error codes
```
