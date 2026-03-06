package com.keboola.jdbc.http;

import com.keboola.jdbc.http.model.KaiResponse;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link KaiClient} SSE parsing logic.
 */
class KaiClientTest {

    private KaiClient client;

    @BeforeEach
    void setUp() {
        client = new KaiClient("https://kai.example.com", "test-token", "https://connection.example.com");
    }

    @Test
    void parseSseResponse_singleTextEvent() throws IOException {
        String sse = "event: text\ndata: {\"text\": \"Hello world\"}\n\nevent: finish\ndata: {}\n\n";
        ResponseBody body = ResponseBody.create(sse, null);

        KaiResponse response = client.parseSseResponse(body);

        assertFalse(response.hasError());
        assertEquals("Hello world", response.getFullText());
        assertNull(response.getExtractedSql());
    }

    @Test
    void parseSseResponse_multipleTextEvents() throws IOException {
        String sse = "event: text\ndata: {\"text\": \"Hello \"}\n\n"
                + "event: text\ndata: {\"text\": \"world\"}\n\n"
                + "event: finish\ndata: {}\n\n";
        ResponseBody body = ResponseBody.create(sse, null);

        KaiResponse response = client.parseSseResponse(body);

        assertFalse(response.hasError());
        assertEquals("Hello world", response.getFullText());
    }

    @Test
    void parseSseResponse_withSqlBlock() throws IOException {
        String sse = "event: text\ndata: {\"text\": \"Here is your SQL:\\n```sql\\nSELECT 1\\n```\"}\n\n"
                + "event: finish\ndata: {}\n\n";
        ResponseBody body = ResponseBody.create(sse, null);

        KaiResponse response = client.parseSseResponse(body);

        assertFalse(response.hasError());
        assertEquals("SELECT 1", response.getExtractedSql());
    }

    @Test
    void parseSseResponse_errorEvent() throws IOException {
        String sse = "event: error\ndata: {\"message\": \"Something went wrong\"}\n\n";
        ResponseBody body = ResponseBody.create(sse, null);

        KaiResponse response = client.parseSseResponse(body);

        assertTrue(response.hasError());
        assertEquals("Something went wrong", response.getErrorMessage());
    }

    @Test
    void parseSseResponse_emptyResponse() throws IOException {
        String sse = "event: finish\ndata: {}\n\n";
        ResponseBody body = ResponseBody.create(sse, null);

        KaiResponse response = client.parseSseResponse(body);

        assertTrue(response.hasError());
        assertTrue(response.getErrorMessage().contains("empty"));
    }

    @Test
    void parseSseResponse_textBeforeError() throws IOException {
        String sse = "event: text\ndata: {\"text\": \"partial\"}\n\n"
                + "event: error\ndata: {\"message\": \"timeout\"}\n\n";
        ResponseBody body = ResponseBody.create(sse, null);

        KaiResponse response = client.parseSseResponse(body);

        assertTrue(response.hasError());
        assertEquals("timeout", response.getErrorMessage());
    }

    @Test
    void parseSseResponse_ignoresToolCallEvents() throws IOException {
        String sse = "event: tool_call\ndata: {\"name\": \"search\"}\n\n"
                + "event: text\ndata: {\"text\": \"Answer\"}\n\n"
                + "event: finish\ndata: {}\n\n";
        ResponseBody body = ResponseBody.create(sse, null);

        KaiResponse response = client.parseSseResponse(body);

        assertFalse(response.hasError());
        assertEquals("Answer", response.getFullText());
    }

    @Test
    void parseSseResponse_noFinishEvent() throws IOException {
        // Stream ends without explicit finish - should still collect text
        String sse = "event: text\ndata: {\"text\": \"Hello\"}\n\n";
        ResponseBody body = ResponseBody.create(sse, null);

        KaiResponse response = client.parseSseResponse(body);

        assertFalse(response.hasError());
        assertEquals("Hello", response.getFullText());
    }

    @Test
    void parseSseResponse_malformedDataLine() throws IOException {
        // Data line with invalid JSON should be skipped gracefully
        String sse = "event: text\ndata: not-json\n\n"
                + "event: text\ndata: {\"text\": \"valid\"}\n\n"
                + "event: finish\ndata: {}\n\n";
        ResponseBody body = ResponseBody.create(sse, null);

        KaiResponse response = client.parseSseResponse(body);

        assertFalse(response.hasError());
        assertEquals("valid", response.getFullText());
    }
}
