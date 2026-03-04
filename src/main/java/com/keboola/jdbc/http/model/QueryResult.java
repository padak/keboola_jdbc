package com.keboola.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Response from GET /api/v1/queries/{queryJobId}/{statementId}/results.
 * Contains a page of result rows along with column metadata and row count information.
 * Rows are returned as lists of strings; null values are represented as JSON null.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryResult {

    /** Result status, typically "completed". */
    @JsonProperty("status")
    private String status;

    /** Ordered list of column descriptors matching the data arrays. */
    @JsonProperty("columns")
    private List<ResultColumn> columns;

    /**
     * Paged result rows. Each row is a list of string values aligned to {@link #columns}.
     * NULL values in the source data are represented as JSON null elements.
     */
    @JsonProperty("data")
    private List<List<String>> data;

    /** Number of rows affected (for DML statements; null for SELECT). */
    @JsonProperty("rowsAffected")
    private Integer rowsAffected;

    /** Total number of rows in the full result set (before paging). */
    @JsonProperty("numberOfRows")
    private Integer numberOfRows;

    /**
     * Indicates whether there are additional pages to fetch.
     * When true, callers should request the next page with an incremented offset.
     */
    @JsonProperty("hasMorePages")
    private boolean hasMorePages;

    /** No-arg constructor required by Jackson. */
    public QueryResult() {}

    public QueryResult(String status, List<ResultColumn> columns, List<List<String>> data,
                       Integer rowsAffected, Integer numberOfRows) {
        this.status       = status;
        this.columns      = columns;
        this.data         = data;
        this.rowsAffected = rowsAffected;
        this.numberOfRows = numberOfRows;
        this.hasMorePages = false;
    }

    public QueryResult(String status, List<ResultColumn> columns, List<List<String>> data,
                       Integer rowsAffected, Integer numberOfRows, boolean hasMorePages) {
        this.status       = status;
        this.columns      = columns;
        this.data         = data;
        this.rowsAffected = rowsAffected;
        this.numberOfRows = numberOfRows;
        this.hasMorePages = hasMorePages;
    }

    public String getStatus() {
        return status;
    }

    public List<ResultColumn> getColumns() {
        return columns;
    }

    public List<List<String>> getData() {
        return data;
    }

    public Integer getRowsAffected() {
        return rowsAffected;
    }

    public Integer getNumberOfRows() {
        return numberOfRows;
    }

    /** Returns true if there are more result pages available for fetching. */
    public boolean isHasMorePages() {
        return hasMorePages;
    }
}
