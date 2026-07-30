package com.keboola.jdbc.auth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for AuthMode - verifies the mapping between the 'auth' connection
 * property and the driver's authentication modes.
 */
class AuthModeTest {

    @Test
    void propertyValue_isTheDocumentedPropertyName() {
        assertEquals("token", AuthMode.STORAGE_TOKEN.propertyValue());
        assertEquals("pat", AuthMode.PAT.propertyValue());
    }

    @Test
    void fromPropertyValue_token_resolvesToStorageToken() {
        assertEquals(AuthMode.STORAGE_TOKEN, AuthMode.fromPropertyValue("token"));
    }

    @Test
    void fromPropertyValue_pat_resolvesToPat() {
        assertEquals(AuthMode.PAT, AuthMode.fromPropertyValue("pat"));
    }

    @Test
    void fromPropertyValue_isCaseInsensitive() {
        assertEquals(AuthMode.PAT, AuthMode.fromPropertyValue("PAT"));
        assertEquals(AuthMode.PAT, AuthMode.fromPropertyValue("PaT"));
        assertEquals(AuthMode.STORAGE_TOKEN, AuthMode.fromPropertyValue("Token"));
    }

    @Test
    void fromPropertyValue_trimsSurroundingWhitespace() {
        assertEquals(AuthMode.PAT, AuthMode.fromPropertyValue("  pat  "));
    }

    @Test
    void fromPropertyValue_unknownValue_returnsNull() {
        assertNull(AuthMode.fromPropertyValue("oauth"));
        assertNull(AuthMode.fromPropertyValue("storage_token"));
        assertNull(AuthMode.fromPropertyValue(""));
        assertNull(AuthMode.fromPropertyValue("   "));
    }

    @Test
    void fromPropertyValue_null_returnsNull() {
        assertNull(AuthMode.fromPropertyValue(null));
    }

    @Test
    void everyModeIsReachableThroughItsPropertyValue() {
        for (AuthMode mode : AuthMode.values()) {
            assertEquals(mode, AuthMode.fromPropertyValue(mode.propertyValue()),
                    "Mode " + mode + " must round-trip through its property value");
        }
    }
}
