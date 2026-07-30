package com.keboola.jdbc.auth;

import com.keboola.jdbc.exception.KeboolaJdbcException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PatAuthProvider - verifies the bearer and project headers,
 * credential detection, and rejection of unusable arguments.
 */
class PatAuthProviderTest {

    private static final String PAT = "kbc_pat_0123456789abcdef";
    private static final long PROJECT_ID = 1234L;

    @Test
    void authHeaders_containBearerCredentialAndProjectId() throws KeboolaJdbcException {
        Map<String, String> headers = new PatAuthProvider(PAT, PROJECT_ID).authHeaders();

        assertEquals(2, headers.size());
        assertEquals("Bearer " + PAT, headers.get(PatAuthProvider.HEADER_AUTHORIZATION));
        assertEquals("1234", headers.get(PatAuthProvider.HEADER_PROJECT_ID));
    }

    @Test
    void headerNames_matchTheServerContract() {
        assertEquals("Authorization", PatAuthProvider.HEADER_AUTHORIZATION);
        assertEquals("X-KBC-ProjectId", PatAuthProvider.HEADER_PROJECT_ID);
    }

    @Test
    void authHeaders_areNotModifiableByCallers() throws KeboolaJdbcException {
        Map<String, String> headers = new PatAuthProvider(PAT, PROJECT_ID).authHeaders();

        assertThrows(UnsupportedOperationException.class, () -> headers.put("X-Evil", "value"));
    }

    @Test
    void mode_isPat() {
        assertEquals(AuthMode.PAT, new PatAuthProvider(PAT, PROJECT_ID).mode());
    }

    @Test
    void getProjectId_returnsTheConfiguredProject() {
        assertEquals(PROJECT_ID, new PatAuthProvider(PAT, PROJECT_ID).getProjectId());
    }

    @Test
    void projectIdBeyondIntegerRange_isPreserved() throws KeboolaJdbcException {
        long largeId = Integer.MAX_VALUE + 7L;

        PatAuthProvider provider = new PatAuthProvider(PAT, largeId);

        assertEquals(largeId, provider.getProjectId());
        assertEquals(String.valueOf(largeId),
                provider.authHeaders().get(PatAuthProvider.HEADER_PROJECT_ID));
    }

    // -------------------------------------------------------------------------
    // looksLikePat()
    // -------------------------------------------------------------------------

    @Test
    void patPrefix_isTheKeboolaIssuedPrefix() {
        assertEquals("kbc_pat_", PatAuthProvider.PAT_PREFIX);
    }

    @Test
    void looksLikePat_prefixedCredential_isTrue() {
        assertTrue(PatAuthProvider.looksLikePat(PAT));
        assertTrue(PatAuthProvider.looksLikePat(PatAuthProvider.PAT_PREFIX));
    }

    @Test
    void looksLikePat_ignoresSurroundingWhitespace() {
        assertTrue(PatAuthProvider.looksLikePat("  " + PAT));
        assertTrue(PatAuthProvider.looksLikePat("\t" + PAT + "\n"));
    }

    @Test
    void looksLikePat_storageApiToken_isFalse() {
        assertFalse(PatAuthProvider.looksLikePat("1234-abcdefghijklmnopqrstuvwxyz"));
    }

    @Test
    void looksLikePat_prefixInWrongCase_isFalse() {
        // Keboola issues the prefix in lower case; a differently cased value is not a PAT.
        assertFalse(PatAuthProvider.looksLikePat("KBC_PAT_0123456789abcdef"));
    }

    @Test
    void looksLikePat_prefixNotAtTheStart_isFalse() {
        assertFalse(PatAuthProvider.looksLikePat("prefixed-kbc_pat_0123456789abcdef"));
    }

    @Test
    void looksLikePat_nullOrBlank_isFalse() {
        assertFalse(PatAuthProvider.looksLikePat(null));
        assertFalse(PatAuthProvider.looksLikePat(""));
        assertFalse(PatAuthProvider.looksLikePat("   "));
    }

    // -------------------------------------------------------------------------
    // Constructor validation
    // -------------------------------------------------------------------------

    @Test
    void constructor_nullPat_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new PatAuthProvider(null, PROJECT_ID));
    }

    @Test
    void constructor_emptyPat_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new PatAuthProvider("", PROJECT_ID));
    }

    @Test
    void constructor_blankPat_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new PatAuthProvider("   ", PROJECT_ID));
    }

    @Test
    void constructor_zeroProjectId_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new PatAuthProvider(PAT, 0));
    }

    @Test
    void constructor_negativeProjectId_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new PatAuthProvider(PAT, -1));
    }

    @Test
    void constructor_rejectionMessage_doesNotExposeThePat() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new PatAuthProvider(PAT, 0)
        );
        assertFalse(ex.getMessage().contains(PAT),
                "Validation message must not leak the Personal Access Token");
    }

    // -------------------------------------------------------------------------
    // toString()
    // -------------------------------------------------------------------------

    @Test
    void toString_doesNotExposeThePat() {
        String str = new PatAuthProvider(PAT, PROJECT_ID).toString();

        assertFalse(str.contains(PAT), "toString() must not expose the Personal Access Token");
        assertTrue(str.contains("redacted"), "toString() should mark the credential as redacted");
        assertTrue(str.contains("1234"), "toString() should report the project ID");
    }
}
