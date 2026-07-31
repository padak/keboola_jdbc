package com.keboola.jdbc.auth;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Authenticates with a project-scoped Keboola Storage API token.
 *
 * <p>The token already carries its project, so no project header is needed.
 */
public class StorageTokenAuthProvider implements AuthProvider {

    /** Storage API and Query Service both accept this header (matching is case-insensitive). */
    public static final String HEADER_STORAGE_TOKEN = "X-StorageApi-Token";

    private final Map<String, String> headers;

    /**
     * @param token Keboola Storage API token; must not be null or blank
     */
    public StorageTokenAuthProvider(String token) {
        Objects.requireNonNull(token, "token");
        if (token.trim().isEmpty()) {
            throw new IllegalArgumentException("Storage API token must not be blank");
        }
        this.headers = Collections.unmodifiableMap(
                Collections.singletonMap(HEADER_STORAGE_TOKEN, token));
    }

    @Override
    public Map<String, String> authHeaders() {
        return headers;
    }

    @Override
    public AuthMode mode() {
        return AuthMode.STORAGE_TOKEN;
    }

    @Override
    public String toString() {
        return "StorageTokenAuthProvider{token=<redacted>}";
    }
}
