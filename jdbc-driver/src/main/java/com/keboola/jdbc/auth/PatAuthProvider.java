package com.keboola.jdbc.auth;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Authenticates with a user-scoped Personal Access Token ({@code kbc_pat_} prefix).
 *
 * <p>A PAT spans every project its scope allows, so the target project cannot be inferred
 * from the credential — Connection resolves it per request from {@code X-KBC-ProjectId}
 * and rejects a project outside the token's scope with 403.
 */
public class PatAuthProvider implements AuthProvider {

    public static final String HEADER_AUTHORIZATION = "Authorization";
    public static final String HEADER_PROJECT_ID    = "X-KBC-ProjectId";

    /** Prefix Keboola issues for Personal Access Tokens; used to auto-detect the auth mode. */
    public static final String PAT_PREFIX = "kbc_pat_";

    private final Map<String, String> headers;
    private final long projectId;

    /**
     * @param pat       Personal Access Token; must not be null or blank
     * @param projectId project the connection operates on; must be positive
     */
    public PatAuthProvider(String pat, long projectId) {
        Objects.requireNonNull(pat, "pat");
        if (pat.trim().isEmpty()) {
            throw new IllegalArgumentException("Personal Access Token must not be blank");
        }
        if (projectId <= 0) {
            throw new IllegalArgumentException("projectId must be positive, got: " + projectId);
        }
        this.projectId = projectId;

        Map<String, String> h = new LinkedHashMap<>();
        h.put(HEADER_AUTHORIZATION, "Bearer " + pat);
        h.put(HEADER_PROJECT_ID, String.valueOf(projectId));
        this.headers = Collections.unmodifiableMap(h);
    }

    /** True if the credential looks like a Personal Access Token. */
    public static boolean looksLikePat(String credential) {
        return credential != null && credential.trim().startsWith(PAT_PREFIX);
    }

    /** Returns the project this connection is scoped to. */
    public long getProjectId() {
        return projectId;
    }

    @Override
    public Map<String, String> authHeaders() {
        return headers;
    }

    @Override
    public AuthMode mode() {
        return AuthMode.PAT;
    }

    @Override
    public String toString() {
        return "PatAuthProvider{pat=<redacted>, projectId=" + projectId + "}";
    }
}
