# JDBC Driver Usage Tracking via Snowflake QUERY_TAG

- **Date:** 2026-07-24
- **Status:** Approved (design)
- **Scope:** `jdbc-driver/` only (VSCode extension is a follow-up)

## Brief

> "Find a simple way to track usage of this tool — how many people use it, what
> actions they perform, and how successful they are."

This design is the **first step** toward that goal: make the driver's queries
identifiable and attributable in Snowflake so usage can be measured from
`QUERY_HISTORY`.

## Background

The driver never talks to Snowflake directly. It POSTs SQL statements to the
Keboola **Query Service**, which executes them in a Keboola-managed **workspace**
that runs on Snowflake. Today the requests carry only the `X-StorageAPI-Token`
header and a JSON body of statements — there is **no marker** distinguishing
driver traffic from any other Query Service traffic, so usage cannot be tracked.

Two facts make `QUERY_TAG` viable:

1. **One session per connection.** `KeboolaConnection` generates a single
   `sessionId` (UUID) at connect and sends it with every job
   (`KeboolaConnection.java:104`). The server-side session "persists SET
   variables, USE SCHEMA, temp tables" for the connection's lifetime
   (`KeboolaConnection.java:78-80`). So a `QUERY_TAG` set **once at connect**
   persists for every query on that connection.
2. **Identity is already known at connect.** `StorageApiClient.verifyToken()`
   returns a `TokenInfo` with `id` (token id), `description`, and `owner`
   (project `id` + `name`). No extra API calls are needed.

`QUERY_HISTORY` already provides the other two parts of the brief for free:
`QUERY_TEXT` + `QUERY_TYPE` ("what actions") and `EXECUTION_STATUS` /
`ERROR_CODE` / `ERROR_MESSAGE` ("how successful"). The tag therefore only needs
to carry an **app marker + identity**.

## Goal

After a connection is established, every query it runs carries a Snowflake
`QUERY_TAG` that identifies:

- that the query came from the Keboola JDBC driver (and which version), and
- the **token** and **project** it originated from.

Non-goal for this step: reading/aggregating the data, dashboards, the Datadog
path, and the VSCode extension.

## Design

### Tag content

The tag is a compact JSON string stored in the session `QUERY_TAG`:

```json
{"app":"kbc-jdbc","v":"2.1.4","tokenId":"12345","projectId":6789}
```

| Field | Source | Notes |
|---|---|---|
| `app` | constant `"kbc-jdbc"` | Stable discriminator for driver traffic. |
| `v` | `DriverConfig.DRIVER_VERSION` | Driver version (already read from `version.properties`). |
| `tokenId` | `TokenInfo.getId()` | Token **id**, string. **Never the token secret.** |
| `projectId` | `TokenInfo.getOwner().getId()` | Project (owner) id, numeric. |

- Serialized with the existing Jackson `ObjectMapper` (guarantees correct
  escaping); do **not** hand-concatenate JSON.
- Well under Snowflake's 2000-char `QUERY_TAG` limit.
- Query it in Snowflake via `PARSE_JSON(query_tag):app = 'kbc-jdbc'`, then group
  by `tokenId` / `projectId`.

**Robustness:** if `owner` is null, omit `projectId` (do not emit `null` that
breaks grouping). If `TokenInfo` itself is somehow unavailable, still emit
`app` + `v` so driver traffic remains distinguishable.

### Where it is set

In `KeboolaConnection` initialization, immediately after workspace resolution
and alongside the existing initial `USE SCHEMA` block (`KeboolaConnection.java:111-113`):

1. Build the tag JSON from the `TokenInfo` already obtained during `verifyToken()`.
2. Submit `ALTER SESSION SET QUERY_TAG='<json>'` as a statement on the
   connection's `sessionId` (same mechanism as the initial `USE SCHEMA`).

Because the session persists, this runs **once per connection**, not per query.

### Helper

Add a small helper (in `KeboolaConnection`, or a static method on
`DriverConfig` / a tiny dedicated class) with signature roughly:

```java
static String buildQueryTag(TokenInfo tokenInfo)   // returns the JSON string
```

Constants (`app` value = `"kbc-jdbc"`, JSON field names) live in `DriverConfig`
per the project's "all constants in DriverConfig" rule.

### Failure handling

Tagging is **telemetry, not correctness**. The `ALTER SESSION SET QUERY_TAG`
call is wrapped in its own `try/catch`. On any failure:

- log a **warning** (not error), and
- **continue** connection setup normally.

A failure to tag must never break `connect()` or query execution.

### Verification (resolves the pass-through unknown)

The one thing not verifiable from this repo is whether the Query Service passes
`ALTER SESSION SET QUERY_TAG` through to Snowflake and does not overwrite it per
job. This is verified empirically by an E2E integration test (see below), not
assumed.

## Testing

Unit tests (JUnit 5 + Mockito, run by `mvn test`):

- `buildQueryTag()` produces the expected JSON for a normal `TokenInfo`.
- `buildQueryTag()` omits `projectId` when `owner` is null, and still emits
  `app` + `v` when identity is missing.
- JSON is correctly escaped (e.g. token description or values with quotes — even
  though current fields are id/version, the serializer path is asserted).
- On connect, the driver issues an `ALTER SESSION SET QUERY_TAG` statement
  (verify via mocked `QueryServiceClient`).
- A thrown exception from the tag statement is swallowed and connect still
  succeeds.

E2E integration test (`KeboolaDriverIT`, `mvn verify -Pkeboola-integration`,
skips without `KEBOOLA_TOKEN`):

- After connect, run `SHOW PARAMETERS LIKE 'QUERY_TAG'` and assert the returned
  value equals the expected JSON (proves pass-through).
- Run a second query, then re-check `SHOW PARAMETERS LIKE 'QUERY_TAG'` and assert
  the tag is unchanged (proves the Query Service does not clobber/reset it per
  job).

If the E2E check shows the tag does **not** survive, the pass-through assumption
is wrong and we escalate to the Query Service team before shipping — the unit
work still stands but the value is not realized until the server cooperates.

## Out of scope (follow-ups)

- **VSCode extension** — mirror this once the JDBC implementation is the proven
  reference (`ls/driver.ts` opens a session too).
- **Datadog / central path** — a `User-Agent` (or `X-KBC-Client`) header plus
  Query-Service-side logging. Deliberately skipped; leave at most a one-line
  hook comment.
- **Reading/aggregating usage** — dashboards and analysis over `QUERY_HISTORY`.

## Risks

- **Pass-through / clobbering:** Query Service may strip or overwrite
  `QUERY_TAG`. Mitigated by the E2E verification gate above.
- **Session re-use edge cases:** if a connection's session is ever recreated
  server-side mid-life, the tag would be lost until re-set. Current design sets
  it only at connect; acceptable for a first step (out-of-scope to detect
  session resets).
