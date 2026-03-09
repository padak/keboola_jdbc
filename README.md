# Keboola Database Connectivity

Monorepo for tools that connect SQL clients to [Keboola](https://www.keboola.com/) projects via the Query Service API.

## Projects

### [jdbc-driver/](jdbc-driver/) -- Keboola JDBC Driver

JDBC driver for DBeaver, DataGrip, and any JDBC-compatible client. Provides auto-discovery of branches and workspaces, virtual `_keboola.*` tables, `KEBOOLA HELP` command, and full Snowflake metadata via `SHOW` commands.

```bash
cd jdbc-driver
mvn clean package          # Build uber-jar
mvn test                   # Run unit tests
make dist                  # Copy jar to dist/
```

See [jdbc-driver/README.md](jdbc-driver/) for full documentation (connection setup, DBeaver config, virtual tables, architecture, changelog).

### vscode-sqltools/ -- VSCode SQLTools Extension (coming soon)

TypeScript extension for Visual Studio Code that integrates with the [SQLTools](https://marketplace.visualstudio.com/items?itemName=mtxr.sqltools) ecosystem. Will provide the same Keboola connectivity directly in VSCode.

## Repository Structure

```
keboola_jdbc/
├── jdbc-driver/           # Java JDBC driver
│   ├── src/               # Java source and tests
│   ├── dist/              # Release jars (all versions)
│   ├── pom.xml            # Maven build
│   └── Makefile           # Build shortcuts
├── vscode-sqltools/       # VSCode SQLTools extension (planned)
├── docs/                  # Project documentation and plans
├── CLAUDE.md              # AI coding instructions
└── README.md              # This file
```
