package com.keboola.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response from POST /api/v1/branches/{branchId}/workspaces/{workspaceId}/queries.
 * Contains the job ID and session ID needed for subsequent status polling and result fetching.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryJob {

    @JsonProperty("queryJobId")
    private String queryJobId;

    @JsonProperty("sessionId")
    private String sessionId;

    /** No-arg constructor required by Jackson. */
    public QueryJob() {}

    public QueryJob(String queryJobId, String sessionId) {
        this.queryJobId = queryJobId;
        this.sessionId = sessionId;
    }

    public String getQueryJobId() {
        return queryJobId;
    }

    public String getSessionId() {
        return sessionId;
    }
}
