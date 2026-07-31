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

**Download:** Grab the latest `keboola-jdbc-driver-X.Y.Z.jar` from the [GitHub Releases page](https://github.com/keboola/jdbc-driver/releases/latest). Older builds remain available in [jdbc-driver/dist/](jdbc-driver/dist/) for reference.

**Installation guides:**
- [DBeaver](docs/dbeaver.md)
- [DataGrip](docs/datagrip.md) (and other JetBrains IDEs)
- [Tableau Desktop](docs/tableau.md)

See [jdbc-driver/](jdbc-driver/) for source and build instructions.

### [vscode-sqltools/](vscode-sqltools/) -- VSCode SQLTools Extension (v2.1.4)

TypeScript extension for Visual Studio Code that integrates with the [SQLTools](https://marketplace.visualstudio.com/items?itemName=mtxr.sqltools) ecosystem. Provides Keboola connectivity directly in VSCode.

**Features:**
- Connect to any Keboola stack (AWS US/EU, GCP, Azure, custom)
- QuickPick selection for project, branch and workspace on first connect
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

## Prerequisites

Both the JDBC driver and the VSCode extension accept two kinds of credentials.

| Credential | Recognized by | Scope | Extra configuration |
|---|---|---|---|
| **Storage API token** | anything without the PAT prefix | a single project | none -- the token carries its project |
| **Personal Access Token** (PAT) | the `kbc_pat_` prefix | your user account, across every project you can reach | the target project, auto-detected when the PAT reaches exactly one |

The driver picks the method from the credential's prefix, so you normally supply just `token`. Set `auth=token` or `auth=pat` if you want to state it explicitly. See [JDBC connection properties](#jdbc-connection-properties) for the full list.

### Storage API Token

Both the JDBC driver and VSCode extension require a Keboola Storage API token. The token **must not** be a bucket-scoped token -- it needs access to workspaces and the Query Service.

**How to create the token:**

1. Go to your Keboola project
2. Navigate to **Settings** > **API Tokens**
3. Click **New Token**
4. Set a description (e.g. "DBeaver access")
5. Under **Access to buckets**, select **All buckets** (or at least the buckets you need to query)
6. Under **Access to components**, you can leave it at **None** (not required for SQL access)
7. Optionally set token expiration
8. Click **Create**

**Minimum required permissions:**
- Read access to buckets you want to query
- The token must be able to list workspaces (`GET /v2/storage/branch/{id}/workspaces`)
- A workspace must already exist in the project (the driver auto-selects the newest one)

**What does NOT work:**
- Bucket-scoped tokens with limited permissions -- the driver needs to list branches and workspaces during connection setup
- Tokens from a different project than the workspace

> **Tip:** If you get connection errors, first verify your token works by visiting `https://connection.keboola.com/v2/storage/tokens/verify` with the header `X-StorageApi-Token: <your-token>`.

### Personal Access Token

A Personal Access Token (PAT) belongs to your Keboola **user**, not to a single project. One PAT therefore works across every project you can reach, and you pick the project per connection instead of creating a token per project.

**How to create one:** open your user **Settings** in the Keboola UI and create a Personal Access Token. Its value starts with `kbc_pat_`.

**Requirements:**
- PATs must be enabled on your Keboola stack (the `programmatic-auth` stack feature). When they are not, the endpoint the driver uses does not exist and the driver fails the connection with an explicit message saying Personal Access Tokens are not enabled on the stack -- ask Keboola support to enable it, or use a Storage API token.
- The project you connect to must be in the token's scope. A project outside it is rejected with HTTP 403.
- A workspace must already exist in the project, exactly as with a Storage API token.

**Choosing the project:**
- If the PAT reaches exactly one project, the driver detects it and logs which project it selected.
- If it reaches several, the connection fails with the list of available `id (name)` pairs and you set the `project` property to one of them.

> **Tip:** to see the projects a PAT can reach, call `https://connection.keboola.com/v1/auth/pat` with the header `Authorization: Bearer <your-pat>`. A `404` from that URL means PATs are not enabled on the stack.

## JDBC connection properties

Every property can be supplied either as a JDBC `Properties` entry (in the client's connection form) or as a URL query parameter. When both carry the same key, the `Properties` entry wins. Prefer the connection form for the credential -- clients commonly store the JDBC URL as plaintext.

| Property | Required | Description |
|---|---|---|
| `token` | yes | Storage API token or Personal Access Token |
| `password` | -- | Accepted as a carrier for `token`, for clients such as Tableau whose JDBC connector has no UI for custom properties. An explicit `token` wins |
| `auth` | no | `token` or `pat`. Default: inferred from the credential -- the `kbc_pat_` prefix means `pat`, anything else means `token` |
| `project` | only for a PAT reaching several projects | Numeric Keboola project ID. Auto-detected when the PAT reaches exactly one project. Ignored with a warning when the credential is a Storage API token, which already carries its project |
| `branch` | no | Branch ID. Default: the project's default branch |
| `workspace` | no | Workspace ID. Default: the newest workspace in the project |
| `schema` | no | Default schema (bucket) for unqualified table references |

**Storage API token:**

```
jdbc:keboola://connection.keboola.com
    token = <your-storage-api-token>
```

**Personal Access Token, project auto-detected:**

```
jdbc:keboola://connection.keboola.com
    token = kbc_pat_<your-personal-access-token>
```

**Personal Access Token, explicit project and workspace:**

```
jdbc:keboola://connection.keboola.com
    token     = kbc_pat_<your-personal-access-token>
    project   = 1234
    workspace = 567890
```

The same connections as a single URL, for clients that accept nothing else:

```
jdbc:keboola://connection.keboola.com?token=<your-storage-api-token>
jdbc:keboola://connection.keboola.com?token=kbc_pat_<your-personal-access-token>&project=1234
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
