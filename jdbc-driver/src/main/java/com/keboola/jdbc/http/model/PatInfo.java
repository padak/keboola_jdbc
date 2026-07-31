package com.keboola.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * One entry of the {@code items} array returned by GET /v1/auth/pat.
 * Describes a Personal Access Token and the projects it currently grants access to.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PatInfo {

    @JsonProperty("id")
    private String id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("scope")
    private Scope scope;

    /**
     * Live resolved access set — populated even when {@link Scope#isAll()} is true,
     * so it is the authoritative list of reachable projects.
     */
    @JsonProperty("projects")
    private List<ProjectAccess> projects;

    @JsonProperty("readOnly")
    private boolean readOnly;

    @JsonProperty("expiresAt")
    private String expiresAt;

    /** No-arg constructor required by Jackson. */
    public PatInfo() {}

    public PatInfo(String id, String name, Scope scope, List<ProjectAccess> projects,
                   boolean readOnly, String expiresAt) {
        this.id = id;
        this.name = name;
        this.scope = scope;
        this.projects = projects;
        this.readOnly = readOnly;
        this.expiresAt = expiresAt;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Scope getScope() {
        return scope;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public String getExpiresAt() {
        return expiresAt;
    }

    /** Returns the projects this token can reach; empty when the response omitted them. */
    public List<ProjectAccess> getAccessibleProjects() {
        return projects != null ? Collections.unmodifiableList(projects) : Collections.emptyList();
    }

    /**
     * Scope of a Personal Access Token: either every project the user is a member of,
     * or an explicit selection.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Scope {

        @JsonProperty("all")
        private boolean all;

        /** No-arg constructor required by Jackson. */
        public Scope() {}

        public Scope(boolean all) {
            this.all = all;
        }

        /** True when the token covers every project of its owner. */
        public boolean isAll() {
            return all;
        }
    }

    /**
     * A single project reachable with the token, with the role the owner holds there.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ProjectAccess {

        /** Project ids can exceed the range of {@code int}. */
        @JsonProperty("id")
        private long id;

        @JsonProperty("name")
        private String name;

        @JsonProperty("role")
        private String role;

        /** No-arg constructor required by Jackson. */
        public ProjectAccess() {}

        public ProjectAccess(long id, String name, String role) {
            this.id = id;
            this.name = name;
            this.role = role;
        }

        public long getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getRole() {
            return role;
        }
    }
}
