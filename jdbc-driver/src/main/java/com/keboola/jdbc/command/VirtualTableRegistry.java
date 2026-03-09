package com.keboola.jdbc.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.keboola.jdbc.ArrayResultSet;
import com.keboola.jdbc.KeboolaConnection;
import com.keboola.jdbc.http.JobQueueClient;
import com.keboola.jdbc.http.StorageApiClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Registry of virtual _keboola.* tables. Each table name maps to a provider
 * that fetches data from Keboola APIs and returns an ArrayResultSet.
 */
public final class VirtualTableRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualTableRegistry.class);

    private VirtualTableRegistry() {}

    /**
     * Queries a virtual table by name.
     *
     * @param tableName  the virtual table name (e.g. "components", "events", "jobs")
     * @param limit      maximum number of rows to return
     * @param connection the parent connection providing API access
     * @return a ResultSet with the virtual table data
     * @throws SQLException if the table name is unknown or the API call fails
     */
    public static ResultSet query(String tableName, int limit, KeboolaConnection connection)
            throws SQLException {
        try {
            switch (tableName) {
                case "components":
                    return queryComponents(connection);
                case "events":
                    return queryEvents(limit, connection);
                case "jobs":
                    return queryJobs(limit, connection);
                case "tables":
                    return queryTables(connection);
                case "buckets":
                    return queryBuckets(connection);
                default:
                    throw new SQLException(
                            "Unknown virtual table: _keboola." + tableName
                            + ". Available: components, events, jobs, tables, buckets"
                    );
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Error querying _keboola." + tableName + ": " + e.getMessage(), e);
        }
    }

    private static ResultSet queryComponents(KeboolaConnection connection) throws Exception {
        StorageApiClient storage = connection.getStorageClient();
        List<JsonNode> components = storage.listComponents();

        List<String> columns = Arrays.asList(
                "component_id", "component_name", "component_type",
                "config_id", "config_name", "config_description",
                "version", "created", "created_by", "is_disabled", "is_deleted"
        );
        List<List<Object>> rows = new ArrayList<>();

        for (JsonNode comp : components) {
            String compId = comp.path("id").asText("");
            String compName = comp.path("name").asText("");
            String compType = comp.path("type").asText("");

            JsonNode configs = comp.get("configurations");
            if (configs != null && configs.isArray()) {
                for (JsonNode cfg : configs) {
                    String configId = cfg.path("id").asText("");
                    String configName = cfg.path("name").asText("");
                    String configDesc = cfg.path("description").asText("");
                    int version = cfg.path("version").asInt(0);
                    String created = cfg.path("created").asText("");
                    String createdBy = cfg.path("creatorToken").path("description").asText("");
                    boolean isDisabled = cfg.path("isDisabled").asBoolean(false);
                    boolean isDeleted = cfg.path("isDeleted").asBoolean(false);

                    rows.add(Arrays.asList(
                            compId, compName, compType,
                            configId, configName, configDesc,
                            version, created, createdBy,
                            isDisabled ? "true" : "false",
                            isDeleted ? "true" : "false"
                    ));
                }
            }
        }

        LOG.debug("_keboola.components: {} rows", rows.size());
        return new ArrayResultSet(columns, rows);
    }

    private static ResultSet queryEvents(int limit, KeboolaConnection connection) throws Exception {
        StorageApiClient storage = connection.getStorageClient();
        List<JsonNode> events = storage.listEvents(limit);

        List<String> columns = Arrays.asList("event_id", "type", "component", "message", "created");
        List<List<Object>> rows = new ArrayList<>();

        for (JsonNode event : events) {
            String id = event.path("uuid").asText(event.path("id").asText(""));
            String type = event.path("type").asText("");
            String component = event.path("component").asText("");
            String message = event.path("message").asText("");
            String created = event.path("created").asText("");
            rows.add(Arrays.asList(id, type, component, message, created));
        }

        LOG.debug("_keboola.events: {} rows", rows.size());
        return new ArrayResultSet(columns, rows);
    }

    private static ResultSet queryJobs(int limit, KeboolaConnection connection) throws Exception {
        JobQueueClient jobClient = connection.getJobQueueClient();
        List<JsonNode> jobs = jobClient.listJobs(limit);

        List<String> columns = Arrays.asList(
                "job_id", "component_id", "config_id", "status",
                "created", "started", "finished", "duration_sec"
        );
        List<List<Object>> rows = new ArrayList<>();

        for (JsonNode job : jobs) {
            String id = job.path("id").asText("");
            String componentId = job.path("component").asText("");
            String configId = job.path("config").asText("");
            String status = job.path("status").asText("");
            String created = job.path("createdTime").asText("");
            String started = job.path("startTime").asText("");
            String finished = job.path("endTime").asText("");
            Object durationSec = job.has("durationSeconds") ? job.get("durationSeconds").asLong(0) : "";

            rows.add(Arrays.asList(id, componentId, configId, status, created, started, finished, durationSec));
        }

        LOG.debug("_keboola.jobs: {} rows", rows.size());
        return new ArrayResultSet(columns, rows);
    }

    private static ResultSet queryTables(KeboolaConnection connection) throws Exception {
        StorageApiClient storage = connection.getStorageClient();
        List<JsonNode> tables = storage.listAllTables();

        List<String> columns = Arrays.asList(
                "table_id", "bucket_id", "name", "primary_key",
                "rows_count", "data_size_bytes", "last_import_date", "created"
        );
        List<List<Object>> rows = new ArrayList<>();

        for (JsonNode table : tables) {
            String id = table.path("id").asText("");
            String bucketId = table.path("bucket").path("id").asText(
                    table.path("bucketId").asText("")
            );
            String name = table.path("name").asText("");

            String primaryKey = "";
            JsonNode pkNode = table.get("primaryKey");
            if (pkNode != null && pkNode.isArray()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < pkNode.size(); i++) {
                    if (i > 0) sb.append(", ");
                    sb.append(pkNode.get(i).asText());
                }
                primaryKey = sb.toString();
            }

            long rowsCount = table.path("rowsCount").asLong(0);
            long dataSize = table.path("dataSizeBytes").asLong(0);
            String lastImport = table.path("lastImportDate").asText("");
            String created = table.path("created").asText("");

            rows.add(Arrays.asList(id, bucketId, name, primaryKey, rowsCount, dataSize, lastImport, created));
        }

        LOG.debug("_keboola.tables: {} rows", rows.size());
        return new ArrayResultSet(columns, rows);
    }

    private static ResultSet queryBuckets(KeboolaConnection connection) throws Exception {
        StorageApiClient storage = connection.getStorageClient();
        List<JsonNode> buckets = storage.listBucketsRaw();

        List<String> columns = Arrays.asList(
                "bucket_id", "name", "stage", "description",
                "tables_count", "data_size_bytes", "created"
        );
        List<List<Object>> rows = new ArrayList<>();

        for (JsonNode bucket : buckets) {
            String id = bucket.path("id").asText("");
            String name = bucket.path("name").asText("");
            String stage = bucket.path("stage").asText("");
            String description = bucket.path("description").asText("");
            int tablesCount = 0;
            JsonNode tablesNode = bucket.get("tables");
            if (tablesNode != null && tablesNode.isArray()) {
                tablesCount = tablesNode.size();
            }
            long dataSize = bucket.path("dataSizeBytes").asLong(0);
            String created = bucket.path("created").asText("");

            rows.add(Arrays.asList(id, name, stage, description, tablesCount, dataSize, created));
        }

        LOG.debug("_keboola.buckets: {} rows", rows.size());
        return new ArrayResultSet(columns, rows);
    }
}
