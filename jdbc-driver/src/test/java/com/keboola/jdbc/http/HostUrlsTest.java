package com.keboola.jdbc.http;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the URL rules that keep a credential off the wire in cleartext: plaintext
 * {@code http://} survives only for a loopback host, everything else is upgraded to
 * {@code https://}.
 */
class HostUrlsTest {

    private static final String PATH = "/v2/storage";

    // ---------------------------------------------------------------------
    // Bare host and explicit https
    // ---------------------------------------------------------------------

    @Test
    void bareHost_getsHttpsScheme() {
        assertEquals("https://connection.keboola.com/v2/storage",
                HostUrls.resolve("connection.keboola.com", PATH));
    }

    @Test
    void bareHostWithPort_getsHttpsScheme() {
        assertEquals("https://connection.keboola.com:8443/v2/storage",
                HostUrls.resolve("connection.keboola.com:8443", PATH));
    }

    @Test
    void httpsBaseUrl_isKept() {
        assertEquals("https://connection.keboola.com/v2/storage",
                HostUrls.resolve("https://connection.keboola.com", PATH));
    }

    // ---------------------------------------------------------------------
    // Trailing slash
    // ---------------------------------------------------------------------

    @Test
    void trailingSlashOnHttpsBase_doesNotProduceDoubleSlash() {
        assertEquals("https://connection.keboola.com/v2/storage",
                HostUrls.resolve("https://connection.keboola.com/", PATH));
    }

    @Test
    void trailingSlashOnBareHost_doesNotProduceDoubleSlash() {
        assertEquals("https://connection.keboola.com/v2/storage",
                HostUrls.resolve("connection.keboola.com/", PATH));
    }

    @Test
    void trailingSlashOnLoopbackBase_doesNotProduceDoubleSlash() {
        assertEquals("http://localhost:1234/v2/storage",
                HostUrls.resolve("http://localhost:1234/", PATH));
    }

    // ---------------------------------------------------------------------
    // Plaintext http is honored for loopback
    // ---------------------------------------------------------------------

    @Test
    void httpLocalhost_staysPlaintext() {
        assertEquals("http://localhost/v2/storage", HostUrls.resolve("http://localhost", PATH));
        assertEquals("http://localhost:1234/v2/storage",
                HostUrls.resolve("http://localhost:1234", PATH));
    }

    @Test
    void httpIpv4Loopback_staysPlaintext() {
        assertEquals("http://127.0.0.1/v2/storage", HostUrls.resolve("http://127.0.0.1", PATH));
        assertEquals("http://127.0.0.1:8080/v2/storage",
                HostUrls.resolve("http://127.0.0.1:8080", PATH));
    }

    @Test
    void httpBracketedIpv6Loopback_staysPlaintext() {
        assertEquals("http://[::1]/v2/storage", HostUrls.resolve("http://[::1]", PATH));
        assertEquals("http://[::1]:8080/v2/storage", HostUrls.resolve("http://[::1]:8080", PATH));
    }

    @Test
    void httpBareIpv6LoopbackWithPort_staysPlaintext() {
        // Unbracketed "::1" is only recognized when a port follows, because the port is split
        // off at the last colon. Bracketed form is the spelling to use.
        assertEquals("http://::1:8080/v2/storage", HostUrls.resolve("http://::1:8080", PATH));
    }

    @Test
    void httpBareIpv6LoopbackWithoutPort_isUpgraded() {
        // Splitting at the last colon leaves ":" here, which matches no loopback spelling, so
        // this fails closed to https rather than sending the credential in cleartext.
        assertEquals("https://::1/v2/storage", HostUrls.resolve("http://::1", PATH));
    }

    // ---------------------------------------------------------------------
    // Plaintext http to anything else is upgraded
    // ---------------------------------------------------------------------

    @Test
    void httpRemoteHost_isUpgradedToHttps() {
        assertEquals("https://connection.keboola.com/v2/storage",
                HostUrls.resolve("http://connection.keboola.com", PATH));
    }

    @Test
    void httpRemoteHostWithPort_isUpgradedKeepingPort() {
        assertEquals("https://connection.keboola.com:8080/v2/storage",
                HostUrls.resolve("http://connection.keboola.com:8080", PATH));
    }

    @Test
    void hostnameMerelyStartingWithLoopbackName_isUpgraded() {
        assertEquals("https://localhost.evil.example.com/v2/storage",
                HostUrls.resolve("http://localhost.evil.example.com", PATH));
        assertEquals("https://127.0.0.1.evil.example.com/v2/storage",
                HostUrls.resolve("http://127.0.0.1.evil.example.com", PATH));
    }

    @Test
    void loopbackNameInUserInfo_isUpgraded() {
        assertEquals("https://localhost@evil.example.com/v2/storage",
                HostUrls.resolve("http://localhost@evil.example.com", PATH));
    }

    @Test
    void loopbackNameInPath_doesNotMakeTheHostLoopback() {
        assertEquals("https://evil.example.com/localhost/v2/storage",
                HostUrls.resolve("http://evil.example.com/localhost", PATH));
    }

    // ---------------------------------------------------------------------
    // isLoopbackBaseUrl
    // ---------------------------------------------------------------------

    @Test
    void isLoopbackBaseUrl_recognizesLoopbackSpellings() {
        assertTrue(HostUrls.isLoopbackBaseUrl("http://localhost"));
        assertTrue(HostUrls.isLoopbackBaseUrl("http://localhost:1234"));
        assertTrue(HostUrls.isLoopbackBaseUrl("http://127.0.0.1"));
        assertTrue(HostUrls.isLoopbackBaseUrl("http://127.0.0.1:8080/some/path"));
        assertTrue(HostUrls.isLoopbackBaseUrl("http://[::1]"));
        assertTrue(HostUrls.isLoopbackBaseUrl("http://[::1]:8080"));
    }

    @Test
    void isLoopbackBaseUrl_rejectsRemoteHosts() {
        assertFalse(HostUrls.isLoopbackBaseUrl("http://connection.keboola.com"));
        assertFalse(HostUrls.isLoopbackBaseUrl("http://localhost.evil.example.com"));
        assertFalse(HostUrls.isLoopbackBaseUrl("http://10.0.0.1"));
        assertFalse(HostUrls.isLoopbackBaseUrl("http://127.0.0.2"));
    }
}
