package com.keboola.jdbc.auth;

import com.keboola.jdbc.exception.KeboolaJdbcException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for StorageTokenAuthProvider - verifies the emitted header and
 * rejection of unusable credentials.
 */
class StorageTokenAuthProviderTest {

    private static final String TOKEN = "1234-abcdefghijklmnopqrstuvwxyz";

    @Test
    void authHeaders_containsOnlyTheStorageTokenHeader() throws KeboolaJdbcException {
        Map<String, String> headers = new StorageTokenAuthProvider(TOKEN).authHeaders();

        assertEquals(1, headers.size(), "A Storage API token needs no other header");
        assertEquals(TOKEN, headers.get(StorageTokenAuthProvider.HEADER_STORAGE_TOKEN));
    }

    @Test
    void headerName_isTheStorageApiTokenHeader() {
        assertEquals("X-StorageApi-Token", StorageTokenAuthProvider.HEADER_STORAGE_TOKEN);
    }

    @Test
    void authHeaders_areNotModifiableByCallers() throws KeboolaJdbcException {
        Map<String, String> headers = new StorageTokenAuthProvider(TOKEN).authHeaders();

        assertThrows(UnsupportedOperationException.class, () -> headers.put("X-Evil", "value"));
    }

    @Test
    void mode_isStorageToken() {
        assertEquals(AuthMode.STORAGE_TOKEN, new StorageTokenAuthProvider(TOKEN).mode());
    }

    @Test
    void constructor_nullToken_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new StorageTokenAuthProvider(null));
    }

    @Test
    void constructor_emptyToken_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new StorageTokenAuthProvider(""));
    }

    @Test
    void constructor_blankToken_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new StorageTokenAuthProvider("   "));
    }

    @Test
    void toString_doesNotExposeTheToken() {
        String str = new StorageTokenAuthProvider(TOKEN).toString();

        assertFalse(str.contains(TOKEN), "toString() must not expose the Storage API token");
        assertTrue(str.contains("redacted"), "toString() should mark the credential as redacted");
    }
}
