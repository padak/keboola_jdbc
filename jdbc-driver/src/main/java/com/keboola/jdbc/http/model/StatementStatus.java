package com.keboola.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Status of a single SQL statement within a query job.
 * A job may contain multiple statements (e.g. when multiple SQL statements are submitted).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StatementStatus {

    @JsonProperty("id")
    private String id;

    @JsonProperty("query")
    private String query;

    /** Execution status: created, enqueued, processing, completed, failed, canceled. */
    @JsonProperty("status")
    private String status;

    /** Error message when status is "failed". */
    @JsonProperty("error")
    private String error;

    /** Total number of rows in the result set (SELECT queries). */
    @JsonProperty("numberOfRows")
    private Integer numberOfRows;

    /** Number of rows affected (INSERT/UPDATE/DELETE queries). */
    @JsonProperty("rowsAffected")
    private Integer rowsAffected;

    /** No-arg constructor required by Jackson. */
    public StatementStatus() {}

    public StatementStatus(String id, String query, String status, String error,
                           Integer numberOfRows, Integer rowsAffected) {
        this.id = id;
        this.query = query;
        this.status = status;
        this.error = error;
        this.numberOfRows = numberOfRows;
        this.rowsAffected = rowsAffected;
    }

    public String getId() {
        return id;
    }

    public String getQuery() {
        return query;
    }

    public String getStatus() {
        return status;
    }

    public String getError() {
        return error;
    }

    public Integer getNumberOfRows() {
        return numberOfRows;
    }

    public Integer getRowsAffected() {
        return rowsAffected;
    }

    /** Returns true if this statement has reached a terminal state. */
    public boolean isTerminal() {
        return "completed".equals(status) || "failed".equals(status) || "canceled".equals(status);
    }

    /** Returns true if this statement completed without error. */
    public boolean isSuccessful() {
        return "completed".equals(status);
    }
}
