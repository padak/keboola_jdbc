package com.keboola.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Column definition within a table's metadata response.
 * Type information (type, nullable, length) is provided separately via the
 * columnMetadata map in {@link TableInfo}, keyed by column name, using entries
 * with keys "KBC.datatype.type", "KBC.datatype.nullable", "KBC.datatype.length".
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnInfo {

    @JsonProperty("name")
    private String name;

    /** No-arg constructor required by Jackson. */
    public ColumnInfo() {}

    public ColumnInfo(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
