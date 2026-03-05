package com.keboola.jdbc.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.keboola.jdbc.config.DriverConfig;
import com.keboola.jdbc.exception.KeboolaJdbcException;
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
 * HTTP client for the Keboola Job Queue API.
 * Auth uses the same X-StorageApi-Token header as Storage API.
 */
public class JobQueueClient {

    private static final Logger LOG = LoggerFactory.getLogger(JobQueueClient.class);

    private static final String HEADER_TOKEN = "X-StorageApi-Token";

    private final String baseUrl;
    private final String token;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;

    /**
     * Creates a new Job Queue API client.
     *
     * @param baseUrl base URL of the Job Queue service (e.g. "https://queue.keboola.com")
     * @param token   Keboola Storage API token
     */
    public JobQueueClient(String baseUrl, String token) {
        this.baseUrl = baseUrl;
        this.token = token;

        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(DriverConfig.HTTP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .readTimeout(DriverConfig.HTTP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .writeTimeout(DriverConfig.HTTP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .followRedirects(true)
                .followSslRedirects(true)
                .build();

        this.objectMapper = new ObjectMapper();
    }

    /**
     * Lists recent jobs from the Job Queue API.
     *
     * @param limit maximum number of jobs to return
     * @return list of job JSON nodes
     * @throws KeboolaJdbcException on network or API error
     */
    public List<JsonNode> listJobs(int limit) throws KeboolaJdbcException {
        // /search/jobs replaced /jobs (old endpoint returned 410 since March 2025)
        String url = baseUrl + "/search/jobs?limit=" + limit + "&sortBy=id&sortOrder=desc";
        LOG.info("Listing jobs from {}", url);

        Request request = new Request.Builder()
                .url(url)
                .header(HEADER_TOKEN, token)
                .get()
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            int code = response.code();
            if (code != 200) {
                throw KeboolaJdbcException.connectionFailed(
                        "Job Queue API returned HTTP " + code + " for " + url
                );
            }

            ResponseBody body = response.body();
            String json = body != null ? body.string() : "[]";
            JsonNode root = objectMapper.readTree(json);

            List<JsonNode> jobs = new ArrayList<>();
            if (root.isArray()) {
                for (JsonNode node : root) {
                    jobs.add(node);
                }
            }
            return jobs;
        } catch (IOException e) {
            throw KeboolaJdbcException.connectionFailed(
                    "IO error fetching jobs from " + url, e
            );
        }
    }
}
