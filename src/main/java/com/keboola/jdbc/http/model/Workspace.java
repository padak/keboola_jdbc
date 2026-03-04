package com.keboola.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response item from GET /v2/storage/workspaces.
 * Represents a Keboola workspace where queries are executed.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Workspace {

    @JsonProperty("id")
    private long id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("type")
    private String type;

    @JsonProperty("status")
    private String status;

    @JsonProperty("backend")
    private String backend;

    @JsonProperty("created")
    private String created;

    /** No-arg constructor required by Jackson. */
    public Workspace() {}

    public Workspace(long id, String name, String type, String status, String backend, String created) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.status = status;
        this.backend = backend;
        this.created = created;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getStatus() {
        return status;
    }

    public String getBackend() {
        return backend;
    }

    public String getCreated() {
        return created;
    }
}
