package com.keboola.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Response item from GET /v2/storage/buckets/{id}/tables?include=columns,columnMetadata.
 * Represents a Keboola Storage table with its column definitions and metadata.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableInfo {

    /** Full table ID, e.g. "in.c-main.my_table". */
    @JsonProperty("id")
    private String id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("bucket")
    private BucketRef bucket;

    /** Ordered list of column definitions for this table. */
    @JsonProperty("columns")
    private List<ColumnInfo> columns;

    /**
     * Column-level metadata keyed by column name.
     * Each value is a list of {@link ColumnMetadata} entries.
     * Relevant keys: "KBC.datatype.type", "KBC.datatype.nullable", "KBC.datatype.length".
     */
    @JsonProperty("columnMetadata")
    private Map<String, List<ColumnMetadata>> columnMetadata;

    /** No-arg constructor required by Jackson. */
    public TableInfo() {}

    public TableInfo(String id, String name, BucketRef bucket,
                     List<ColumnInfo> columns, Map<String, List<ColumnMetadata>> columnMetadata) {
        this.id = id;
        this.name = name;
        this.bucket = bucket;
        this.columns = columns;
        this.columnMetadata = columnMetadata;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public BucketRef getBucket() {
        return bucket;
    }

    /**
     * Returns the bucket ID derived from the bucket reference if available,
     * otherwise parses it from the table ID (e.g. "in.c-main" from "in.c-main.my_table").
     */
    public String getBucketId() {
        if (bucket != null && bucket.getId() != null) {
            return bucket.getId();
        }
        // Fallback: extract bucket ID from table ID (everything before the last dot)
        if (id != null) {
            int lastDot = id.lastIndexOf('.');
            if (lastDot > 0) {
                return id.substring(0, lastDot);
            }
        }
        return null;
    }

    public List<ColumnInfo> getColumns() {
        return columns;
    }

    public Map<String, List<ColumnMetadata>> getColumnMetadata() {
        return columnMetadata;
    }

    /**
     * Minimal bucket reference embedded in the table info response.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BucketRef {

        @JsonProperty("id")
        private String id;

        /** No-arg constructor required by Jackson. */
        public BucketRef() {}

        public BucketRef(String id) {
            this.id = id;
        }

        public String getId() {
            return id;
        }
    }
}
