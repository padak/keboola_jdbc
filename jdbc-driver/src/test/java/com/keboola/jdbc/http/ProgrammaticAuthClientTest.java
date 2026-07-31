package com.keboola.jdbc.http;

import com.keboola.jdbc.exception.KeboolaJdbcException;
import com.keboola.jdbc.http.model.PatInfo;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.keboola.jdbc.http.MockServerFixture.jsonResponse;
import static com.keboola.jdbc.http.MockServerFixture.rawResponse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProgrammaticAuthClientTest {

    private static final String PAT = "kbc_pat_test-credential";

    private MockWebServer server;
    private ProgrammaticAuthClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        // Plaintext http is only honored for loopback hosts, and MockWebServer.url() reports
        // whatever name 127.0.0.1 reverse-resolves to — spell the loopback literal instead.
        client = new ProgrammaticAuthClient("http://127.0.0.1:" + server.getPort(), PAT);
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    private static Map<String, Object> project(long id, String name, String role) {
        Map<String, Object> project = new LinkedHashMap<>();
        project.put("id", id);
        project.put("name", name);
        project.put("role", role);
        return project;
    }

    private static Map<String, Object> patItem(String id, String name, Object projects) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("id", id);
        item.put("name", name);
        item.put("scope", Collections.singletonMap("all", true));
        item.put("projects", projects);
        item.put("readOnly", false);
        item.put("expiresAt", "2026-12-31T23:59:59+00:00");
        item.put("createdAt", "2026-01-01T00:00:00+00:00");
        return item;
    }

    private static Map<String, Object> itemsEnvelope(Object items) {
        return Collections.singletonMap("items", items);
    }

    // ---------------------------------------------------------------------
    // Happy path
    // ---------------------------------------------------------------------

    @Test
    void listPersonalAccessTokens_parsesItemsWithResolvedProjects() throws Exception {
        Map<String, Object> item = patItem("pat-uuid", "my laptop",
                Collections.singletonList(project(1234L, "My Project", "admin")));
        server.enqueue(jsonResponse(200, itemsEnvelope(Collections.singletonList(item))));

        List<PatInfo> pats = client.listPersonalAccessTokens();

        assertEquals(1, pats.size());
        PatInfo pat = pats.get(0);
        assertEquals("pat-uuid", pat.getId());
        assertEquals("my laptop", pat.getName());
        assertNotNull(pat.getScope());
        assertTrue(pat.getScope().isAll());
        assertFalse(pat.isReadOnly());
        assertEquals("2026-12-31T23:59:59+00:00", pat.getExpiresAt());

        assertEquals(1, pat.getAccessibleProjects().size());
        PatInfo.ProjectAccess access = pat.getAccessibleProjects().get(0);
        assertEquals(1234L, access.getId());
        assertEquals("My Project", access.getName());
        assertEquals("admin", access.getRole());
    }

    @Test
    void listPersonalAccessTokens_sendsOnlyBearerHeader() throws Exception {
        server.enqueue(jsonResponse(200, itemsEnvelope(Collections.emptyList())));

        client.listPersonalAccessTokens();

        RecordedRequest req = server.takeRequest();
        assertEquals("GET", req.getMethod());
        assertEquals("/v1/auth/pat", req.getPath());
        assertEquals("Bearer " + PAT, req.getHeader("Authorization"));
        assertNull(req.getHeader("X-KBC-ProjectId"),
                "/v1/auth/pat is not a storage route — the project is what this call discovers");
        assertNull(req.getHeader("X-StorageApi-Token"));
    }

    @Test
    void listPersonalAccessTokens_multipleItems_returnsAll() throws Exception {
        Map<String, Object> parent = patItem("parent", "parent token",
                Arrays.asList(project(1L, "Alpha", "admin"), project(2L, "Beta", "guest")));
        Map<String, Object> child = patItem("child", "descendant token",
                Collections.singletonList(project(2L, "Beta", "guest")));
        server.enqueue(jsonResponse(200, itemsEnvelope(Arrays.asList(parent, child))));

        List<PatInfo> pats = client.listPersonalAccessTokens();

        assertEquals(2, pats.size());
        assertEquals(2, pats.get(0).getAccessibleProjects().size());
        assertEquals(1, pats.get(1).getAccessibleProjects().size());
        assertEquals(2L, pats.get(1).getAccessibleProjects().get(0).getId());
    }

    @Test
    void listPersonalAccessTokens_emptyItems_returnsEmptyList() throws Exception {
        server.enqueue(jsonResponse(200, itemsEnvelope(Collections.emptyList())));

        assertTrue(client.listPersonalAccessTokens().isEmpty());
    }

    @Test
    void listPersonalAccessTokens_itemWithoutProjects_returnsEmptyProjectList() throws Exception {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("id", "pat-uuid");
        item.put("name", "no projects resolved");
        server.enqueue(jsonResponse(200, itemsEnvelope(Collections.singletonList(item))));

        List<PatInfo> pats = client.listPersonalAccessTokens();

        assertTrue(pats.get(0).getAccessibleProjects().isEmpty());
    }

    @Test
    void listPersonalAccessTokens_projectIdBeyondIntRange_roundTrips() throws Exception {
        long largeId = ((long) Integer.MAX_VALUE) + 12345L;
        Map<String, Object> item = patItem("pat-uuid", "big ids",
                Collections.singletonList(project(largeId, "Huge Project", "admin")));
        server.enqueue(jsonResponse(200, itemsEnvelope(Collections.singletonList(item))));

        List<PatInfo> pats = client.listPersonalAccessTokens();

        assertEquals(largeId, pats.get(0).getAccessibleProjects().get(0).getId());
    }

    // ---------------------------------------------------------------------
    // Failure mapping
    // ---------------------------------------------------------------------

    @Test
    void listPersonalAccessTokens_http404_reportsFeatureNotEnabled() {
        server.enqueue(jsonResponse(404, Collections.singletonMap("error", "Not Found")));

        KeboolaJdbcException ex = assertThrows(KeboolaJdbcException.class,
                client::listPersonalAccessTokens);

        assertEquals("28000", ex.getSQLState());
        assertTrue(ex.getMessage().contains("not enabled on this Keboola stack"), ex.getMessage());
        assertEquals(1, server.getRequestCount(), "404 must not retry");
    }

    @Test
    void listPersonalAccessTokens_http401_reportsInvalidCredential() {
        server.enqueue(jsonResponse(401, Collections.singletonMap("error", "Unauthorized")));

        KeboolaJdbcException ex = assertThrows(KeboolaJdbcException.class,
                client::listPersonalAccessTokens);

        assertEquals("28000", ex.getSQLState());
        assertTrue(ex.getMessage().contains("invalid, expired or revoked"), ex.getMessage());
        assertEquals(1, server.getRequestCount(), "401 must not retry");
    }

    @Test
    void listPersonalAccessTokens_errorMessagesNeverContainTheCredential() {
        server.enqueue(jsonResponse(401, Collections.singletonMap("error", "Unauthorized")));

        KeboolaJdbcException ex = assertThrows(KeboolaJdbcException.class,
                client::listPersonalAccessTokens);

        assertFalse(ex.getMessage().contains(PAT), "Credential must not leak into error messages");
    }

    @Test
    void listPersonalAccessTokens_http403_throwsAuthenticationFailed() {
        server.enqueue(jsonResponse(403, Collections.singletonMap("error", "Forbidden")));

        KeboolaJdbcException ex = assertThrows(KeboolaJdbcException.class,
                client::listPersonalAccessTokens);

        assertEquals("28000", ex.getSQLState());
        assertEquals(1, server.getRequestCount());
    }

    @Test
    void listPersonalAccessTokens_missingItemsArray_throwsConnectionFailed() {
        server.enqueue(jsonResponse(200, Collections.singletonMap("other", "value")));

        KeboolaJdbcException ex = assertThrows(KeboolaJdbcException.class,
                client::listPersonalAccessTokens);

        assertEquals("08001", ex.getSQLState());
        assertTrue(ex.getMessage().contains("'items'"), ex.getMessage());
    }

    @Test
    void listPersonalAccessTokens_malformedJson_throwsConnectionFailed() {
        server.enqueue(rawResponse(200, "{not json"));

        KeboolaJdbcException ex = assertThrows(KeboolaJdbcException.class,
                client::listPersonalAccessTokens);

        assertEquals("08001", ex.getSQLState());
    }

    // ---------------------------------------------------------------------
    // Retry policy
    // ---------------------------------------------------------------------

    @Test
    void listPersonalAccessTokens_retriesOn500ThenSucceeds() throws Exception {
        server.enqueue(jsonResponse(500, Collections.singletonMap("error", "transient")));
        server.enqueue(jsonResponse(200, itemsEnvelope(Collections.emptyList())));

        assertTrue(client.listPersonalAccessTokens().isEmpty());
        assertEquals(2, server.getRequestCount());
    }

    @Test
    void listPersonalAccessTokens_exhaustsRetriesOn500() {
        server.enqueue(jsonResponse(500, Collections.singletonMap("error", "down")));
        server.enqueue(jsonResponse(500, Collections.singletonMap("error", "down")));
        server.enqueue(jsonResponse(500, Collections.singletonMap("error", "down")));

        KeboolaJdbcException ex = assertThrows(KeboolaJdbcException.class,
                client::listPersonalAccessTokens);

        assertEquals("08001", ex.getSQLState());
        assertEquals(3, server.getRequestCount());
    }

    @Test
    void listPersonalAccessTokens_retriesOn429() throws Exception {
        server.enqueue(rawResponse(429, "Too Many Requests"));
        server.enqueue(jsonResponse(200, itemsEnvelope(Collections.emptyList())));

        client.listPersonalAccessTokens();

        assertEquals(2, server.getRequestCount());
    }
}
