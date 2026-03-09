package com.keboola.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response item from GET /v2/storage/buckets.
 * Represents a Keboola Storage bucket (e.g. "in.c-main").
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Bucket {

    /** Full bucket ID, e.g. "in.c-main". */
    @JsonProperty("id")
    private String id;

    @JsonProperty("name")
    private String name;

    /** Stage of the bucket: "in" (input) or "out" (output). */
    @JsonProperty("stage")
    private String stage;

    @JsonProperty("description")
    private String description;

    @JsonProperty("backend")
    private String backend;

    /** No-arg constructor required by Jackson. */
    public Bucket() {}

    public Bucket(String id, String name, String stage, String description, String backend) {
        this.id = id;
        this.name = name;
        this.stage = stage;
        this.description = description;
        this.backend = backend;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getStage() {
        return stage;
    }

    public String getDescription() {
        return description;
    }

    public String getBackend() {
        return backend;
    }
}
