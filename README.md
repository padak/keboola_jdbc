# Keboola Database Connectivity

Monorepo for tools that connect SQL clients to [Keboola](https://www.keboola.com/) projects via the Query Service API.

## Projects

### [jdbc-driver/](jdbc-driver/) -- Keboola JDBC Driver (v2.1.4)

JDBC driver for DBeaver, DataGrip, and any JDBC-compatible client. Provides auto-discovery of branches and workspaces, virtual `_keboola.*` tables, `KEBOOLA HELP` command, and full Snowflake metadata via `SHOW` commands.

```bash
cd jdbc-driver
mvn clean package          # Build uber-jar
mvn test                   # Run unit tests
make dist                  # Copy jar to dist/
```

**Download:** [jdbc-driver/dist/keboola-jdbc-driver-2.1.4.jar](jdbc-driver/dist/keboola-jdbc-driver-2.1.4.jar)

See [jdbc-driver/README.md](jdbc-driver/) for full documentation.

### [vscode-sqltools/](vscode-sqltools/) -- VSCode SQLTools Extension (v2.1.4)

TypeScript extension for Visual Studio Code that integrates with the [SQLTools](https://marketplace.visualstudio.com/items?itemName=mtxr.sqltools) ecosystem. Provides Keboola connectivity directly in VSCode.

**Features:**
- Connect to any Keboola stack (AWS US/EU, GCP, Azure, custom)
- QuickPick selection for branch and workspace on first connect
- Browse buckets, tables, and columns in the sidebar explorer
- Execute SQL queries with async polling and pagination
- Virtual `_keboola.*` tables (components, events, jobs, tables, buckets)
- `KEBOOLA HELP`, `SHOW HISTORY`, `USE SCHEMA` commands
- Query cancellation from Command Palette
- Schema cache with 60s TTL and stale-on-error fallback

```bash
cd vscode-sqltools
npm install && npm run compile  # Build
npm test                        # Run 147 unit tests
npm run package                 # Create .vsix
```

**Install:** Download the VSIX from [vscode-sqltools/dist/](vscode-sqltools/dist/) and install via:
```bash
code --install-extension vscode-sqltools/dist/sqltools-keboola-driver-2.1.4.vsix
```

## Repository Structure

```
keboola_jdbc/
├── jdbc-driver/               # Java JDBC driver
│   ├── src/                   # Java source and tests
│   ├── dist/                  # Release jars (all versions)
│   ├── pom.xml                # Maven build
│   └── Makefile               # Build shortcuts
├── vscode-sqltools/           # VSCode SQLTools extension
│   ├── src/                   # TypeScript source
│   │   ├── extension.ts       # Extension entry point (QuickPick, registration)
│   │   ├── constants.ts       # Configuration constants
│   │   └── ls/                # Language server (driver, plugin, cache)
│   ├── dist/                  # Release .vsix files (all versions)
│   ├── package.json           # Extension manifest
│   └── connection.schema.json # Connection form schema
├── docs/                      # Project documentation and plans
├── CLAUDE.md                  # AI coding instructions
└── README.md                  # This file
```
