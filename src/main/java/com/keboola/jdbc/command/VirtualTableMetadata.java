package com.keboola.jdbc.command;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Defines the schema metadata for virtual _keboola.* tables.
 * Used by KeboolaDatabaseMetaData to expose virtual tables in the sidebar
 * and autocomplete of DBeaver/DataGrip.
 */
public final class VirtualTableMetadata {

    private VirtualTableMetadata() {}

    /** The virtual schema name that appears in the sidebar. */
    public static final String SCHEMA_NAME = "_keboola";

    /**
     * Maps virtual table name to its column definitions.
     * Each column is defined as [name, jdbcType, typeName, displaySize].
     */
    private static final Map<String, List<Object[]>> TABLE_COLUMNS;

    static {
        Map<String, List<Object[]>> map = new LinkedHashMap<>();

        map.put("components", Arrays.asList(
                col("component_id", Types.VARCHAR, "VARCHAR", 255),
                col("component_name", Types.VARCHAR, "VARCHAR", 255),
                col("type", Types.VARCHAR, "VARCHAR", 50),
                col("config_count", Types.INTEGER, "INTEGER", 10)
        ));

        map.put("events", Arrays.asList(
                col("event_id", Types.VARCHAR, "VARCHAR", 64),
                col("type", Types.VARCHAR, "VARCHAR", 50),
                col("component", Types.VARCHAR, "VARCHAR", 255),
                col("message", Types.VARCHAR, "VARCHAR", 4096),
                col("created", Types.VARCHAR, "TIMESTAMP", 30)
        ));

        map.put("jobs", Arrays.asList(
                col("job_id", Types.VARCHAR, "VARCHAR", 20),
                col("component_id", Types.VARCHAR, "VARCHAR", 255),
                col("config_id", Types.VARCHAR, "VARCHAR", 255),
                col("status", Types.VARCHAR, "VARCHAR", 30),
                col("created", Types.VARCHAR, "TIMESTAMP", 30),
                col("started", Types.VARCHAR, "TIMESTAMP", 30),
                col("finished", Types.VARCHAR, "TIMESTAMP", 30),
                col("duration_sec", Types.INTEGER, "INTEGER", 10)
        ));

        map.put("tables", Arrays.asList(
                col("table_id", Types.VARCHAR, "VARCHAR", 255),
                col("bucket_id", Types.VARCHAR, "VARCHAR", 255),
                col("name", Types.VARCHAR, "VARCHAR", 255),
                col("primary_key", Types.VARCHAR, "VARCHAR", 1024),
                col("rows_count", Types.BIGINT, "BIGINT", 19),
                col("data_size_bytes", Types.BIGINT, "BIGINT", 19),
                col("last_import_date", Types.VARCHAR, "TIMESTAMP", 30),
                col("created", Types.VARCHAR, "TIMESTAMP", 30)
        ));

        map.put("buckets", Arrays.asList(
                col("bucket_id", Types.VARCHAR, "VARCHAR", 255),
                col("name", Types.VARCHAR, "VARCHAR", 255),
                col("stage", Types.VARCHAR, "VARCHAR", 10),
                col("description", Types.VARCHAR, "VARCHAR", 4096),
                col("tables_count", Types.INTEGER, "INTEGER", 10),
                col("data_size_bytes", Types.BIGINT, "BIGINT", 19),
                col("created", Types.VARCHAR, "TIMESTAMP", 30)
        ));

        TABLE_COLUMNS = Collections.unmodifiableMap(map);
    }

    /** Returns all virtual table names. */
    public static List<String> getTableNames() {
        return List.copyOf(TABLE_COLUMNS.keySet());
    }

    /** Returns column definitions for a virtual table, or null if unknown. */
    public static List<Object[]> getColumns(String tableName) {
        return TABLE_COLUMNS.get(tableName);
    }

    private static Object[] col(String name, int jdbcType, String typeName, int displaySize) {
        return new Object[]{name, jdbcType, typeName, displaySize};
    }
}
