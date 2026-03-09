# Keboola SQLTools Driver

A VS Code extension that connects [SQLTools](https://vscode-sqltools.mteixeira.dev/) to [Keboola](https://www.keboola.com/) projects, allowing you to run SQL queries, browse storage metadata, and interact with Keboola-specific features directly from VS Code.

## Features

- **SQL Query Execution** - Run SQL queries against Keboola workspaces via the Query Service API with paginated result fetching
- **Database Explorer** - Browse buckets, tables, and columns with data types in the SQLTools sidebar
- **Auto-Discovery** - Automatically discovers the Query Service URL, default branch, and newest workspace
- **Session Persistence** - Maintains server-side session state across queries using a persistent session ID
- **USE SCHEMA / USE DATABASE** - Set a default schema that is automatically prepended to subsequent queries via multi-statement jobs
- **Virtual Tables** - Query Keboola metadata with SQL syntax (`SELECT * FROM _keboola.components`, `_keboola.events`, `_keboola.jobs`, `_keboola.tables`, `_keboola.buckets`)
- **KEBOOLA HELP** - Display all available Keboola-specific commands
- **SHOW HISTORY** - View workspace query history from the Query Service API
- **Query Cancellation** - Cancel running queries from the VS Code Command Palette
- **Schema Cache** - 60-second TTL cache with stale-on-error fallback for sidebar metadata
- **SQL Autocomplete** - Comprehensive SQL keyword completions plus Keboola commands and virtual table names

## Prerequisites

- [Visual Studio Code](https://code.visualstudio.com/) v1.87.0 or later
- [SQLTools extension](https://marketplace.visualstudio.com/items?itemName=mtxr.sqltools) installed
- A Keboola project with at least one active workspace

## Installation

1. Download the latest `.vsix` file from the releases
2. In VS Code, open the Command Palette (`Cmd+Shift+P` / `Ctrl+Shift+P`)
3. Run **Extensions: Install from VSIX...**
4. Select the downloaded `.vsix` file
5. Reload VS Code when prompted

## Connection Configuration

1. Open the SQLTools sidebar (database icon in the activity bar)
2. Click **Add New Connection**
3. Select **Keboola** as the driver
4. Fill in the connection form:

| Field | Required | Description |
|-------|----------|-------------|
| **Connection Name** | Yes | A display name for this connection |
| **Keboola Stack** | Yes | Select your Keboola stack from the dropdown (e.g., `connection.keboola.com`, `connection.eu-central-1.keboola.com`) or choose `custom` for a custom URL |
| **Custom Connection URL** | Only if stack is `custom` | The full connection URL (e.g., `connection.mycompany.keboola.com`) |
| **Storage API Token** | Yes | Your Keboola Storage API token (found in Keboola UI under Settings > API Tokens) |
| **Branch ID** | No | Specific branch ID. If omitted, the default branch is auto-detected |
| **Workspace ID** | No | Specific workspace ID. If omitted, the newest workspace is auto-selected |

## Usage

### Running Queries

Open any `.sql` file or use the SQLTools query editor, then execute SQL queries as usual:

```sql
SELECT * FROM "in.c-main"."my_table" LIMIT 100;
```

### Setting a Default Schema

Use `USE SCHEMA` to avoid fully qualifying table names in every query:

```sql
USE SCHEMA "in.c-main";
SELECT * FROM "my_table" LIMIT 100;
```

The driver automatically prepends `USE SCHEMA` to subsequent queries so they execute in the same Snowflake session.

### Virtual Tables

Query Keboola project metadata using virtual `_keboola.*` tables:

```sql
-- List all components and configurations
SELECT * FROM _keboola.components;

-- List recent storage events
SELECT * FROM _keboola.events LIMIT 50;

-- List recent jobs
SELECT * FROM _keboola.jobs LIMIT 20;

-- List all tables with metadata
SELECT * FROM _keboola.tables;

-- List all buckets
SELECT * FROM _keboola.buckets;
```

Virtual table queries are handled locally and do not use a Query Service workspace session.

### Keboola Help

Display all available Keboola-specific commands:

```sql
KEBOOLA HELP;
```

### Query History

View recent query history from the workspace:

```sql
SHOW HISTORY;
-- or
SHOW QUERY HISTORY;
```

### Cancelling Queries

To cancel a running query, open the Command Palette (`Cmd+Shift+P` / `Ctrl+Shift+P`) and run:

```
SQLTools: Keboola: Cancel Running Query
```

Queries that exceed the timeout (default: 300 seconds) are automatically cancelled.

### Browsing the Sidebar

The SQLTools sidebar shows your Keboola storage structure:

```
Connection
  +-- in.c-main (bucket)
  |     +-- Tables
  |           +-- customers
  |           |     +-- id (NUMBER)
  |           |     +-- name (VARCHAR)
  |           |     +-- email (VARCHAR)
  |           +-- orders
  +-- out.c-results (bucket)
        +-- Tables
              +-- summary
```

Click on any table to see its columns with data types.

## Development

### Build

```bash
cd vscode-sqltools
npm install
npm run compile
```

### Test

```bash
npm test
```

### Watch Mode

```bash
npm run watch
```

### Package as VSIX

```bash
npm run package
```

This produces a `.vsix` file in the `vscode-sqltools/` directory.

### Project Structure

```
vscode-sqltools/
  src/
    extension.ts          # VS Code entry point
    constants.ts          # All configuration constants
    types.ts              # TypeScript interfaces
    ls/
      plugin.ts           # Language server plugin registration
      driver.ts           # Main driver (connection, query, sidebar)
      queries.ts          # IBaseQueries stubs (metadata via REST API)
      schema-cache.ts     # Metadata cache with TTL + stale-on-error
      virtual-tables.ts   # _keboola.* virtual table handlers
      help-command.ts     # KEBOOLA HELP command handler
    test/
      suite/              # Unit tests
  connection.schema.json  # Connection form JSON Schema
  ui.schema.json          # Connection form UI layout
  icons/                  # Extension icons
  package.json            # Extension manifest
  tsconfig.json           # TypeScript configuration
```
