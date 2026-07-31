# Tableau Desktop Installation Guide

This guide walks you through installing the Keboola JDBC driver in [Tableau Desktop](https://www.tableau.com/products/desktop) using the generic **Other Databases (JDBC)** connector.

> Tableau's JDBC connector has no UI for custom driver properties. Supply your credential in the **Password** field — the driver accepts it there so the secret stays out of the JDBC URL. (Tableau stores the URL as plaintext in saved/published data sources, but treats the password as a credential.) Anything that is *not* a credential, such as `project`, has to ride in the URL query string.

## Prerequisites

- Tableau Desktop 2021.1 or newer (older versions have an unreliable JDBC connector)
- One of:
  - a Keboola Storage API token with workspace access — see [Storage API Token](../README.md#storage-api-token)
  - a Personal Access Token — see [Personal Access Token](../README.md#personal-access-token)

## 1. Download the driver

Download the latest `keboola-jdbc-driver-X.Y.Z.jar` from the [GitHub Releases page](https://github.com/keboola/jdbc-driver/releases/latest).

## 2. Install the driver

Place the jar in Tableau's drivers directory:

| OS | Path |
|---|---|
| macOS | `~/Library/Tableau/Drivers/` |
| Windows | `C:\Program Files\Tableau\Drivers\` |
| Linux | `/opt/tableau/tableau_driver/jdbc/` |

Create the directory if it doesn't exist. **Restart Tableau Desktop** after dropping the jar — Tableau only scans the drivers folder at startup.

<!-- TODO screenshot: Tableau_drivers_folder.png — Finder/Explorer window showing the keboola-jdbc-driver jar dropped into ~/Library/Tableau/Drivers -->
![Drivers folder with the Keboola jar](Tableau_drivers_folder.png)

## 3. Create a connection

1. In Tableau, choose **Connect → To a Server → More… → Other Databases (JDBC)**.

   <!-- TODO screenshot: Tableau_connect_menu.png — Tableau start page with "More…" expanded under "To a Server", highlighting "Other Databases (JDBC)" -->
   ![Connect menu — Other Databases (JDBC)](Tableau_connect_menu.png)

2. Fill in the connection form:
   - **URL:**
     ```
     jdbc:keboola://connection.keboola.com
     ```
     Replace the host with your Keboola stack (e.g. `connection.eu-central-1.keboola.com`). Optional: append `?branch=<id>&workspace=<id>` to pin a specific branch or workspace; both are auto-detected if omitted.
   - **Dialect:** `SQL92`
   - **Username:** leave empty.
   - **Password:** your Storage API token or Personal Access Token. The driver reads the credential from the password field, keeping it out of the (plaintext) URL, and recognizes a Personal Access Token by its `kbc_pat_` prefix.

   Using a Personal Access Token that reaches more than one project, add the project to the URL — the Password field is the only credential input Tableau offers, so `project` cannot go anywhere else:

   ```
   jdbc:keboola://connection.keboola.com?project=1234
   ```

   A Personal Access Token that reaches exactly one project needs no `project` at all; the driver detects it.

   <!-- TODO screenshot: Tableau_connection_form.png — Filled-in Other Databases (JDBC) dialog with the jdbc:keboola:// URL and SQL92 dialect selected; redact the token before screenshotting -->
   ![JDBC connection form](Tableau_connection_form.png)

3. Click **Sign In**.

See [`ConnectionConfig.java`](../jdbc-driver/src/main/java/com/keboola/jdbc/config/ConnectionConfig.java) for the full property list.

## 4. First query

In the data source pane, switch to **New Custom SQL** and run:

```sql
SELECT * FROM _keboola.buckets LIMIT 10
```

This returns one row per bucket in your project and confirms the driver is wired up. `_keboola.buckets` is one of five virtual tables exposing Keboola platform metadata (`components`, `events`, `jobs`, `tables`, `buckets`).

To browse real Snowflake tables, pick a database and schema from the left-hand pane — Tableau will populate them via standard JDBC metadata calls.

## Troubleshooting

- **"Required driver not found"** — the jar isn't in the Tableau drivers directory, or Tableau wasn't restarted after dropping it in. Confirm the path matches the table above and relaunch Tableau.
- **Authentication or 403 errors with a Storage API token** — your token is likely bucket-scoped. Verify by hitting `https://connection.keboola.com/v2/storage/tokens/verify` with header `X-StorageApi-Token: <your-token>`; a bucket-scoped token will not see workspaces. Create a non-scoped token per [the README](../README.md#storage-api-token).
- **Authentication or 403 errors with a Personal Access Token** — either the token is outside the project's scope, or `project` names a project the token cannot reach. List what it can reach with `https://connection.keboola.com/v1/auth/pat` and header `Authorization: Bearer <your-pat>`; a `404` there means Personal Access Tokens are not enabled on the stack.
- **Ambiguous project** — a Personal Access Token reaching several projects without `project` in the URL fails with the list of available projects. Pick one and append `?project=<id>`.
- **"No workspaces found"** — the project has no workspace yet. Open the project in Keboola UI and create a workspace (Transformations → Workspaces).
- **Custom stack** — replace the host in the JDBC URL with your stack hostname (e.g. `jdbc:keboola://connection.north-europe.azure.keboola.com?token=…`).
- **Generic "An error occurred" with no details** — Tableau swallows JDBC error messages. Run the same connection in DBeaver to see the real error, then fix and retry in Tableau.
