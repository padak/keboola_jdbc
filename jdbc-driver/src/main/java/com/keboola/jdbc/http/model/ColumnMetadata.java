package com.keboola.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A single metadata entry for a column.
 * Metadata keys of interest include:
 * - "KBC.datatype.type"     - the SQL data type (e.g. "VARCHAR", "INTEGER")
 * - "KBC.datatype.nullable" - whether the column allows NULL values ("true"/"false")
 * - "KBC.datatype.length"   - the type length/precision (e.g. "255")
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnMetadata {

    @JsonProperty("key")
    private String key;

    @JsonProperty("value")
    private String value;

    @JsonProperty("provider")
    private String provider;

    /** No-arg constructor required by Jackson. */
    public ColumnMetadata() {}

    public ColumnMetadata(String key, String value, String provider) {
        this.key = key;
        this.value = value;
        this.provider = provider;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getProvider() {
        return provider;
    }
}
