package com.keboola.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response item from GET /v2/storage/dev-branches/.
 * Represents a Keboola development branch.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Branch {

    @JsonProperty("id")
    private int id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("isDefault")
    private boolean isDefault;

    @JsonProperty("created")
    private String created;

    @JsonProperty("description")
    private String description;

    /** No-arg constructor required by Jackson. */
    public Branch() {}

    public Branch(int id, String name, boolean isDefault, String created, String description) {
        this.id = id;
        this.name = name;
        this.isDefault = isDefault;
        this.created = created;
        this.description = description;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public boolean isDefault() {
        return isDefault;
    }

    public String getCreated() {
        return created;
    }

    public String getDescription() {
        return description;
    }
}
