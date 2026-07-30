package com.keboola.jdbc.http;

/**
 * Builds absolute URLs for the Keboola Connection host shared by the Storage API and the
 * programmatic-auth endpoints.
 *
 * <p>Package-private: the normalization rules are a security boundary (a credential is
 * attached to every request built from these URLs) and must stay identical for all clients
 * talking to that host.
 */
final class HostUrls {

    private HostUrls() {
        // utility class
    }

    /**
     * Resolves a request URL from a configured host and an absolute path.
     *
     * <p>Accepts either a bare host ("connection.keboola.com") or a full base URL
     * ("http://localhost:1234"); the latter is used by unit tests against MockWebServer.
     * Plaintext {@code http://} is honored only for loopback hosts — a credential is attached
     * to every request, so it must never be sent in cleartext to a remote host and such a
     * base URL is upgraded to {@code https://}.
     *
     * @param host configured host or base URL
     * @param path absolute path starting with "/"
     * @return absolute request URL
     */
    static String resolve(String host, String path) {
        // Strip any trailing slash on the base URL so paths starting with "/" don't
        // produce double slashes (e.g. "http://x/" + "/v2/storage" -> "http://x//v2/storage").
        String base = host;
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        if (base.startsWith("https://")) {
            return base + path;
        }
        if (base.startsWith("http://")) {
            if (isLoopbackBaseUrl(base)) {
                return base + path;
            }
            return "https://" + base.substring("http://".length()) + path;
        }
        return "https://" + base + path;
    }

    /** True if an "http://" base URL points at a loopback host (localhost / 127.0.0.1 / ::1). */
    static boolean isLoopbackBaseUrl(String httpBase) {
        String hostPort = httpBase.substring("http://".length());
        int slash = hostPort.indexOf('/');
        if (slash >= 0) {
            hostPort = hostPort.substring(0, slash);
        }
        int colon = hostPort.lastIndexOf(':');
        // Keep IPv6 brackets intact; only strip a trailing :port.
        String hostOnly = (colon >= 0 && hostPort.indexOf(']') < colon) ? hostPort.substring(0, colon) : hostPort;
        return hostOnly.equals("localhost")
                || hostOnly.equals("127.0.0.1")
                || hostOnly.equals("[::1]")
                || hostOnly.equals("::1");
    }
}
