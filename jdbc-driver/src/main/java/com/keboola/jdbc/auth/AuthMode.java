package com.keboola.jdbc.auth;

/**
 * Authentication mechanism used against the Keboola APIs.
 *
 * <p>The mode is derived from the supplied credential's prefix unless the {@code auth}
 * connection property names it explicitly.
 */
public enum AuthMode {

    /** Project-scoped Storage API token sent in the {@code X-StorageApi-Token} header. */
    STORAGE_TOKEN("token"),

    /**
     * User-scoped Personal Access Token ({@code kbc_pat_} prefix) sent as a bearer
     * credential. Cross-project, so each request also carries {@code X-KBC-ProjectId}.
     */
    PAT("pat");

    private final String propertyValue;

    AuthMode(String propertyValue) {
        this.propertyValue = propertyValue;
    }

    /** Returns the value accepted by the {@code auth} connection property. */
    public String propertyValue() {
        return propertyValue;
    }

    /**
     * Resolves the {@code auth} property value to a mode.
     *
     * @param value property value, case-insensitive
     * @return the matching mode, or null if the value is not recognized
     */
    public static AuthMode fromPropertyValue(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim().toLowerCase(java.util.Locale.ROOT);
        for (AuthMode mode : values()) {
            if (mode.propertyValue.equals(normalized)) {
                return mode;
            }
        }
        return null;
    }
}
