package com.keboola.jdbc.http;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.keboola.jdbc.auth.PatAuthProvider;
import com.keboola.jdbc.config.DriverConfig;
import com.keboola.jdbc.exception.KeboolaJdbcException;
import com.keboola.jdbc.http.model.PatInfo;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * HTTP client for the Keboola programmatic authentication endpoints ({@code /v1/auth/*}).
 *
 * <p>Used to discover which projects a Personal Access Token can reach, before a
 * {@link com.keboola.jdbc.auth.AuthProvider} exists — a PAT needs a target project id and
 * that id is exactly what this lookup provides. The credential is therefore taken directly
 * rather than through a provider.
 *
 * <p>{@code /v1/auth/*} is not a Storage API route, so requests carry only the bearer
 * credential and must not include a project header.
 */
public class ProgrammaticAuthClient {

    private static final Logger LOG = LoggerFactory.getLogger(ProgrammaticAuthClient.class);

    private static final String PATH_LIST_PATS = "/v1/auth/pat";

    private final String       host;
    private final String       personalAccessToken;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;

    /**
     * Creates a new programmatic auth client.
     *
     * @param host                Keboola Connection host, e.g. "connection.keboola.com"
     * @param personalAccessToken Personal Access Token sent as a bearer credential
     */
    public ProgrammaticAuthClient(String host, String personalAccessToken) {
        this.host                = host;
        this.personalAccessToken = personalAccessToken;

        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(DriverConfig.HTTP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .readTimeout(DriverConfig.HTTP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .writeTimeout(DriverConfig.HTTP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .followRedirects(true)
                .followSslRedirects(true)
                .build();

        this.objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Lists the Personal Access Tokens visible to the bearer credential.
     * Endpoint: GET https://{host}/v1/auth/pat
     *
     * <p>With a PAT bearer the result is the calling token plus its descendants. Since a
     * descendant can never exceed its parent, the union of {@code projects} across the
     * returned items is the caller's own accessible project set.
     *
     * @return one entry per visible token; never null
     * @throws KeboolaJdbcException if the credential is rejected, the stack does not support
     *                              programmatic auth, or the response cannot be parsed
     */
    public List<PatInfo> listPersonalAccessTokens() throws KeboolaJdbcException {
        String url = HostUrls.resolve(host, PATH_LIST_PATS);
        LOG.info("Listing Personal Access Tokens from {}", url);
        String body = executeGet(url);
        return parsePatItems(body, url);
    }

    // --- Internal helpers ---

    /**
     * Executes a GET request with exponential back-off retry for transient errors.
     * Retries up to {@link DriverConfig#MAX_RETRIES} times for 5xx and 429 responses;
     * 401 and 404 fail immediately with the reason the caller can act on.
     */
    private String executeGet(String url) throws KeboolaJdbcException {
        Request request = new Request.Builder()
                .url(url)
                .header(PatAuthProvider.HEADER_AUTHORIZATION, "Bearer " + personalAccessToken)
                .get()
                .build();

        int  attempts = 0;
        long delayMs  = DriverConfig.POLL_INITIAL_INTERVAL_MS;

        while (true) {
            attempts++;
            LOG.debug("HTTP GET {} (attempt {}/{})", url, attempts, DriverConfig.MAX_RETRIES);

            try (Response response = httpClient.newCall(request).execute()) {
                int code = response.code();
                LOG.debug("HTTP {} <- {}", code, url);

                if (code == 200) {
                    ResponseBody responseBody = response.body();
                    return responseBody != null ? responseBody.string() : "";
                }

                // Programmatic auth is a gated stack feature; when it is off the route
                // does not exist at all, so 404 is a capability answer, not a wrong URL.
                if (code == 404) {
                    throw KeboolaJdbcException.authenticationFailed(
                            "Personal Access Tokens are not enabled on this Keboola stack (HTTP 404 from "
                                    + url + ")"
                    );
                }
                if (code == 401) {
                    throw KeboolaJdbcException.authenticationFailed(
                            "The Personal Access Token is invalid, expired or revoked (HTTP 401 from "
                                    + url + ")"
                    );
                }
                if (code == 403) {
                    throw KeboolaJdbcException.authenticationFailed(
                            "HTTP " + code + " from " + url
                    );
                }
                if (code == 400) {
                    throw KeboolaJdbcException.connectionFailed(
                            "Bad request (HTTP 400) for " + url
                    );
                }

                boolean retryable = (code == 429 || code >= 500);
                if (!retryable || attempts >= DriverConfig.MAX_RETRIES) {
                    throw KeboolaJdbcException.connectionFailed(
                            "HTTP " + code + " from " + url + " after " + attempts + " attempt(s)"
                    );
                }

                LOG.warn("Retryable HTTP {} from {}; waiting {}ms before retry {}/{}",
                        code, url, delayMs, attempts + 1, DriverConfig.MAX_RETRIES);
                sleepUninterruptibly(delayMs);
                delayMs = Math.min((long) (delayMs * DriverConfig.POLL_BACKOFF_FACTOR),
                        DriverConfig.POLL_MAX_INTERVAL_MS);

            } catch (IOException e) {
                if (attempts >= DriverConfig.MAX_RETRIES) {
                    throw KeboolaJdbcException.connectionFailed(
                            "IO error after " + attempts + " attempt(s) for " + url, e
                    );
                }
                LOG.warn("IO error for {} (attempt {}): {}; retrying in {}ms",
                        url, attempts, e.getMessage(), delayMs);
                sleepUninterruptibly(delayMs);
                delayMs = Math.min((long) (delayMs * DriverConfig.POLL_BACKOFF_FACTOR),
                        DriverConfig.POLL_MAX_INTERVAL_MS);
            }
        }
    }

    /** Reads the {@code items} envelope of the PAT list response. */
    private List<PatInfo> parsePatItems(String responseBody, String requestUrl) throws KeboolaJdbcException {
        try {
            JsonNode items = objectMapper.readTree(responseBody).get("items");
            if (items == null || !items.isArray()) {
                throw KeboolaJdbcException.connectionFailed(
                        "Response from " + requestUrl + " does not contain an 'items' array"
                );
            }
            List<PatInfo> result = new ArrayList<>(items.size());
            for (JsonNode item : items) {
                result.add(objectMapper.treeToValue(item, PatInfo.class));
            }
            return result;
        } catch (IOException e) {
            throw KeboolaJdbcException.connectionFailed(
                    "Failed to parse Personal Access Token list from " + requestUrl, e
            );
        }
    }

    /** Sleeps for the given duration, logging a warning if interrupted. */
    private static void sleepUninterruptibly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Sleep interrupted during retry back-off");
        }
    }
}
