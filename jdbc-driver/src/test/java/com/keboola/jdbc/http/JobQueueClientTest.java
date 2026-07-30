package com.keboola.jdbc.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.keboola.jdbc.auth.PatAuthProvider;
import com.keboola.jdbc.auth.StorageTokenAuthProvider;
import com.keboola.jdbc.exception.KeboolaJdbcException;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.keboola.jdbc.http.MockServerFixture.jsonResponse;
import static com.keboola.jdbc.http.MockServerFixture.rawResponse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobQueueClientTest {

    private MockWebServer server;
    private String baseUrl;
    private JobQueueClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        // strip trailing slash so the client builds the same URL it would in production
        baseUrl = server.url("").toString().replaceAll("/$", "");
        client = new JobQueueClient(baseUrl, new StorageTokenAuthProvider("test-token"));
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    void listJobs_happyPath_returnsParsedArray() throws Exception {
        Map<String, Object> job1 = new HashMap<>();
        job1.put("id", "j1");
        job1.put("status", "success");
        Map<String, Object> job2 = new HashMap<>();
        job2.put("id", "j2");
        job2.put("status", "error");
        server.enqueue(jsonResponse(200, Arrays.asList(job1, job2)));

        List<JsonNode> jobs = client.listJobs(10);

        assertEquals(2, jobs.size());
        assertEquals("j1", jobs.get(0).get("id").asText());
        assertEquals("error", jobs.get(1).get("status").asText());
    }

    @Test
    void listJobs_emptyArray_returnsEmptyList() throws Exception {
        server.enqueue(jsonResponse(200, Collections.emptyList()));

        List<JsonNode> jobs = client.listJobs(10);

        assertTrue(jobs.isEmpty());
    }

    @Test
    void listJobs_nonArrayJson_returnsEmptyList() throws Exception {
        // Defensive: API misbehaves, returns an object instead of an array
        Map<String, Object> wrapped = new HashMap<>();
        wrapped.put("jobs", Collections.emptyList());
        server.enqueue(jsonResponse(200, wrapped));

        List<JsonNode> jobs = client.listJobs(10);

        assertTrue(jobs.isEmpty());
    }

    @Test
    void listJobs_http401_throwsException() {
        server.enqueue(jsonResponse(401, Collections.singletonMap("error", "Invalid token")));

        KeboolaJdbcException ex = assertThrows(KeboolaJdbcException.class, () -> client.listJobs(10));
        assertTrue(ex.getMessage().contains("401"), "Message should mention HTTP code: " + ex.getMessage());
    }

    @Test
    void listJobs_http500_throwsExceptionWithoutRetry() throws Exception {
        // JobQueueClient does not retry — single 5xx should propagate immediately
        server.enqueue(jsonResponse(500, Collections.singletonMap("error", "boom")));

        assertThrows(KeboolaJdbcException.class, () -> client.listJobs(10));
        assertEquals(1, server.getRequestCount(), "Client should not retry on 5xx");
    }

    @Test
    void listJobs_malformedJson_throwsException() {
        server.enqueue(rawResponse(200, "{not json"));

        assertThrows(KeboolaJdbcException.class, () -> client.listJobs(10));
    }

    @Test
    void listJobs_sendsAuthHeaderAndLimitInUrl() throws Exception {
        server.enqueue(jsonResponse(200, Collections.emptyList()));

        client.listJobs(42);

        RecordedRequest req = server.takeRequest();
        assertEquals("GET", req.getMethod());
        assertEquals("test-token", req.getHeader("X-StorageApi-Token"));
        assertNotNull(req.getPath());
        assertTrue(req.getPath().startsWith("/search/jobs"), "Path: " + req.getPath());
        assertTrue(req.getPath().contains("limit=42"), "Path should carry limit=42: " + req.getPath());
        assertTrue(req.getPath().contains("sortBy=id"), "Path should carry sortBy=id: " + req.getPath());
        assertTrue(req.getPath().contains("sortOrder=desc"), "Path should carry sortOrder=desc: " + req.getPath());
    }

    @Test
    void patProvider_sendsBearerAndProjectHeader_withoutStorageTokenHeader() throws Exception {
        JobQueueClient patClient = new JobQueueClient(
                baseUrl, new PatAuthProvider("kbc_pat_secret", 4321L));
        server.enqueue(jsonResponse(200, Collections.emptyList()));

        patClient.listJobs(10);

        RecordedRequest req = server.takeRequest();
        assertEquals("Bearer kbc_pat_secret", req.getHeader("Authorization"));
        assertEquals("4321", req.getHeader("X-KBC-ProjectId"));
        assertNull(req.getHeader("X-StorageApi-Token"));
    }

    @Test
    void authHeaders_areResolvedPerRequest() throws Exception {
        RotatingAuthProvider rotating = new RotatingAuthProvider();
        JobQueueClient rotatingClient = new JobQueueClient(baseUrl, rotating);
        server.enqueue(jsonResponse(200, Collections.emptyList()));
        server.enqueue(jsonResponse(200, Collections.emptyList()));

        rotatingClient.listJobs(10);
        rotatingClient.listJobs(10);

        assertEquals(RotatingAuthProvider.tokenForCall(1),
                server.takeRequest().getHeader("X-StorageApi-Token"));
        assertEquals(RotatingAuthProvider.tokenForCall(2),
                server.takeRequest().getHeader("X-StorageApi-Token"),
                "The second call must carry the freshly resolved credential");
    }
}
