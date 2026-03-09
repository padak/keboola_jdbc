package com.keboola.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Response from GET /api/v1/queries/{queryJobId}.
 * Represents the current execution status of a submitted query job.
 * Valid statuses: created, enqueued, processing, completed, failed, canceled.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobStatus {

    @JsonProperty("queryJobId")
    private String queryJobId;

    /** Job lifecycle status: created, enqueued, processing, completed, failed, canceled. */
    @JsonProperty("status")
    private String status;

    /** Status details for each individual SQL statement in this job. */
    @JsonProperty("statements")
    private List<StatementStatus> statements;

    /** No-arg constructor required by Jackson. */
    public JobStatus() {}

    public JobStatus(String queryJobId, String status, List<StatementStatus> statements) {
        this.queryJobId = queryJobId;
        this.status = status;
        this.statements = statements;
    }

    public String getQueryJobId() {
        return queryJobId;
    }

    public String getStatus() {
        return status;
    }

    public List<StatementStatus> getStatements() {
        return statements;
    }

    /** Returns true if the job has reached a terminal state (completed, failed, or canceled). */
    public boolean isTerminal() {
        return "completed".equals(status) || "failed".equals(status) || "canceled".equals(status);
    }

    /** Returns true if the job completed successfully. */
    public boolean isSuccessful() {
        return "completed".equals(status);
    }
}
