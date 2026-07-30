# DBeaver Installation Guide

This guide walks you through installing the Keboola JDBC driver in [DBeaver](https://dbeaver.io/) and connecting to a Keboola project.

## Prerequisites

- DBeaver Community or PRO (any recent version with JDBC driver support)
- Java 11 or newer available to DBeaver (DBeaver ships its own JRE on most platforms)
- One of:
  - a Keboola Storage API token with workspace access — see [Storage API Token](../README.md#storage-api-token)
  - a Personal Access Token — see [Personal Access Token](../README.md#personal-access-token)

## 1. Download the driver

Download the latest `keboola-jdbc-driver-X.Y.Z.jar` from the [GitHub Releases page](https://github.com/keboola/jdbc-driver/releases/latest).

Save the jar somewhere stable on your machine — e.g. `~/keboola/keboola-jdbc-driver.jar`.

## 2. Register the driver in DBeaver

1. Open **Database → Driver Manager**.
2. Click **New**.

   ![Driver Manager — New driver](DBeaver_driver_1.png)

3. Fill in the driver details:
   - **Driver Name:** `Keboola`
   - **Class Name:** `com.keboola.jdbc.KeboolaDriver`
   - **URL Template:** `jdbc:keboola://{host}`
   - **Default Port:** *(leave empty)*
4. On the **Libraries** tab, click **Add File** and select the jar you downloaded.

   ![Driver Manager — Libraries](DBeaver_driver_2.png)

5. Click **Find Class** to confirm DBeaver detects `com.keboola.jdbc.KeboolaDriver`, then **OK**.

## 3. Create a connection

1. **Database → New Database Connection**, then select **Keboola** from the driver list.

   ![New connection — driver picker](DBeaver_DB_1.png)

2. Fill in the connection form:
   - **JDBC URL:** `jdbc:keboola://connection.keboola.com` (replace the host with your Keboola stack, e.g. `connection.eu-central-1.keboola.com`)
   - **User name:** *(leave empty)*
   - **Password:** *(leave empty)*

3. Open **Driver properties** and add the credential plus any optional overrides:

   | Property | Required | Description |
   |---|---|---|
   | `token` | yes | Your Keboola Storage API token or Personal Access Token |
   | `auth` | no | `token` or `pat`. Auto-detected from the credential — the `kbc_pat_` prefix means `pat` |
   | `project` | only for a Personal Access Token reaching several projects | Numeric project ID. Auto-detected when the token reaches exactly one project; ignored for a Storage API token |
   | `branch` | no | Specific branch ID. Auto-detected (default branch) if omitted |
   | `workspace` | no | Specific workspace ID. Newest workspace is auto-selected if omitted |

   ![Connection — driver properties](DBeaver_DB_2.png)

   Once `token` holds a Personal Access Token, DBeaver offers the reachable projects as a drop-down on `project`; `branch` and `workspace` fill in the same way once the project is known.

   See [`ConnectionConfig.java`](../jdbc-driver/src/main/java/com/keboola/jdbc/config/ConnectionConfig.java) for the full property list.

4. Click **Test Connection**. On success, **Finish**.

## 4. First query

Open a SQL editor against the new connection and run:

```sql
KEBOOLA HELP;

SELECT * FROM _keboola.buckets LIMIT 10;
```

`KEBOOLA HELP` lists every Keboola-specific command. `_keboola.buckets` is one of five virtual tables exposing platform metadata (`components`, `events`, `jobs`, `tables`, `buckets`).

## Troubleshooting

- **"Property 'token' is required"** — the credential wasn't added under Driver properties. Re-open the connection settings and add it.
- **Authentication or 403 errors with a Storage API token** — your token is likely bucket-scoped. Verify by hitting `https://connection.keboola.com/v2/storage/tokens/verify` with header `X-StorageApi-Token: <your-token>`; a bucket-scoped token will not see workspaces. Create a non-scoped token per [the README](../README.md#storage-api-token).
- **Ambiguous project** — a Personal Access Token reaching several projects fails the connection with the list of available projects. Set `project` to one of the listed IDs.
- **"Personal Access Tokens are not enabled on this Keboola stack"** — the stack lacks the `programmatic-auth` feature. Ask Keboola support to enable it, or connect with a Storage API token.
- **"No workspaces found"** — the project has no workspace yet. Open the project in Keboola UI and create a workspace (Transformations → Workspaces).
- **Custom stack** — replace the host in the JDBC URL with your stack hostname (e.g. `jdbc:keboola://connection.north-europe.azure.keboola.com`).
