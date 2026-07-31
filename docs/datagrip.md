# DataGrip Installation Guide

This guide walks you through installing the Keboola JDBC driver in [DataGrip](https://www.jetbrains.com/datagrip/) and connecting to a Keboola project. The same flow works for any JetBrains IDE with the **Database Tools and SQL** plugin (IntelliJ Ultimate, PyCharm Professional, etc.).

## Prerequisites

- DataGrip 2023.1 or newer
- One of:
  - a Keboola Storage API token with workspace access — see [Storage API Token](../README.md#storage-api-token)
  - a Personal Access Token — see [Personal Access Token](../README.md#personal-access-token)

## 1. Download the driver

Download the latest `keboola-jdbc-driver-X.Y.Z.jar` from the [GitHub Releases page](https://github.com/keboola/jdbc-driver/releases/latest).

Save it somewhere stable on your machine — e.g. `~/keboola/keboola-jdbc-driver.jar`.

## 2. Register the driver in DataGrip

1. Open the **Database** tool window (View → Tool Windows → Database).
2. Click the **+** icon → **Driver**.

   ![Driver settings](DataGrip_driver_1.png)

3. Fill in the driver details:
   - **Name:** `Keboola`
   - **Driver Files:** click **+** → **Custom JARs…** and pick the downloaded jar.
   - **Class:** select `com.keboola.jdbc.KeboolaDriver` from the dropdown (DataGrip scans the jar automatically).
   - **URL templates:** add `jdbc:keboola://{host}`
   - **Dialect:** `Generic SQL` (or `Snowflake` if you prefer that completion grammar — the driver targets Snowflake under the hood).

4. **Apply** and **OK**.

## 3. Create a connection

1. In the Database tool window, click **+** → **Data Source → Keboola**.

   ![New data source](DataGrip_DB_1.png)

2. Fill in the connection form:
   - **URL:** `jdbc:keboola://connection.keboola.com` (replace the host with your Keboola stack, e.g. `connection.eu-central-1.keboola.com`)
   - **User / Password:** leave empty.

3. Open the **Advanced** tab and add these properties:

   | Property | Required | Description |
   |---|---|---|
   | `token` | yes | Your Keboola Storage API token or Personal Access Token |
   | `auth` | no | `token` or `pat`. Auto-detected from the credential — the `kbc_pat_` prefix means `pat` |
   | `project` | only for a Personal Access Token reaching several projects | Numeric project ID. Auto-detected when the token reaches exactly one project; ignored for a Storage API token |
   | `branch` | no | Specific branch ID. Auto-detected (default branch) if omitted |
   | `workspace` | no | Specific workspace ID. Newest workspace is auto-selected if omitted |

   With a Personal Access Token in `token`, DataGrip offers the reachable projects as a drop-down on `project`; `branch` and `workspace` populate the same way once the project is known.

   See [`ConnectionConfig.java`](../jdbc-driver/src/main/java/com/keboola/jdbc/config/ConnectionConfig.java) for the full property list.

4. Click **Test Connection**. On success, **OK**.

## 4. First query

Open a query console against the new data source and run:

```sql
KEBOOLA HELP;

SELECT * FROM _keboola.buckets LIMIT 10;
```

`KEBOOLA HELP` lists every Keboola-specific command. `_keboola.buckets` is one of five virtual tables exposing platform metadata (`components`, `events`, `jobs`, `tables`, `buckets`).

## Troubleshooting

- **"Property 'token' is required"** — the credential wasn't added under the **Advanced** tab. Re-open the data source and add it.
- **Authentication or 403 errors with a Storage API token** — your token is likely bucket-scoped. Verify by hitting `https://connection.keboola.com/v2/storage/tokens/verify` with header `X-StorageApi-Token: <your-token>`; a bucket-scoped token will not see workspaces. Create a non-scoped token per [the README](../README.md#storage-api-token).
- **Ambiguous project** — a Personal Access Token reaching several projects fails the connection with the list of available projects. Set `project` to one of the listed IDs.
- **"Personal Access Tokens are not enabled on this Keboola stack"** — the stack lacks the `programmatic-auth` feature. Ask Keboola support to enable it, or connect with a Storage API token.
- **"No workspaces found"** — the project has no workspace yet. Open the project in Keboola UI and create a workspace (Transformations → Workspaces).
- **Custom stack** — replace the host in the JDBC URL with your stack hostname (e.g. `jdbc:keboola://connection.north-europe.azure.keboola.com`).
