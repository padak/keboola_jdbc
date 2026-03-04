package com.keboola.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response model from GET /v2/storage/tokens/verify.
 * Represents an authenticated Keboola Storage API token with its associated metadata.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TokenInfo {

    @JsonProperty("id")
    private String id;

    @JsonProperty("description")
    private String description;

    @JsonProperty("canManageBuckets")
    private boolean canManageBuckets;

    @JsonProperty("owner")
    private Owner owner;

    @JsonProperty("defaultBackend")
    private String defaultBackend;

    /** No-arg constructor required by Jackson. */
    public TokenInfo() {}

    public TokenInfo(String id, String description, boolean canManageBuckets, Owner owner, String defaultBackend) {
        this.id = id;
        this.description = description;
        this.canManageBuckets = canManageBuckets;
        this.owner = owner;
        this.defaultBackend = defaultBackend;
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }

    public boolean isCanManageBuckets() {
        return canManageBuckets;
    }

    public Owner getOwner() {
        return owner;
    }

    public String getDefaultBackend() {
        return defaultBackend;
    }

    /**
     * Nested owner object within the token info response.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Owner {

        @JsonProperty("id")
        private int id;

        @JsonProperty("name")
        private String name;

        /** No-arg constructor required by Jackson. */
        public Owner() {}

        public Owner(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }
}
