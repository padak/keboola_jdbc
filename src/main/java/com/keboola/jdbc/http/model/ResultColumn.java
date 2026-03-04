package com.keboola.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Column descriptor in a query result response.
 * Provides the name, SQL type, nullability, length, precision, and scale for each result column.
 *
 * Note: {@code nullable} is modeled as a {@link Boolean} object (not primitive) to allow
 * distinguishing "explicitly not nullable" from "nullability unknown" (null).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ResultColumn {

    @JsonProperty("name")
    private String name;

    /** SQL data type name, e.g. "VARCHAR", "INTEGER", "TIMESTAMP". */
    @JsonProperty("type")
    private String type;

    /**
     * Whether the column allows NULL values.
     * Null means the nullability is unknown (maps to {@code columnNullableUnknown} in JDBC).
     */
    @JsonProperty("nullable")
    private Boolean nullable;

    /** Maximum length for variable-length types; null for fixed-size types. */
    @JsonProperty("length")
    private Integer length;

    /** Numeric precision for decimal/numeric types; null for non-numeric types. */
    @JsonProperty("precision")
    private Integer precision;

    /** Numeric scale (digits after decimal point) for decimal/numeric types; null for others. */
    @JsonProperty("scale")
    private Integer scale;

    /** No-arg constructor required by Jackson. */
    public ResultColumn() {}

    /**
     * Convenience constructor for results without precision/scale info.
     * Precision and scale default to null.
     */
    public ResultColumn(String name, String type, boolean nullable, Integer length) {
        this.name     = name;
        this.type     = type;
        this.nullable = nullable;
        this.length   = length;
    }

    /** Full constructor including precision and scale. */
    public ResultColumn(String name, String type, Boolean nullable, Integer length,
                        Integer precision, Integer scale) {
        this.name      = name;
        this.type      = type;
        this.nullable  = nullable;
        this.length    = length;
        this.precision = precision;
        this.scale     = scale;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    /**
     * Returns nullability as a {@link Boolean} object.
     * {@code null} indicates that nullability is unknown.
     */
    public Boolean isNullable() {
        return nullable;
    }

    public Integer getLength() {
        return length;
    }

    public Integer getPrecision() {
        return precision;
    }

    public Integer getScale() {
        return scale;
    }
}
