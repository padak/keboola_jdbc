package com.keboola.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Backend context returned by the Query Service in result responses.
 * Contains the current catalog (database) and schema after query execution,
 * as reported by Snowflake's finalDatabaseName / finalSchemaName.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackendContext {

    @JsonProperty("catalog")
    private String catalog;

    @JsonProperty("schema")
    private String schema;

    public BackendContext() {}

    public BackendContext(String catalog, String schema) {
        this.catalog = catalog;
        this.schema = schema;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getSchema() {
        return schema;
    }
}
