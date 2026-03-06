package com.keboola.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.keboola.jdbc.config.DriverConfig;
import com.keboola.jdbc.exception.KeboolaJdbcException;
import com.keboola.jdbc.http.model.KaiResponse;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * HTTP client for the Keboola Kai AI assistant API.
 * Sends chat messages and collects SSE (Server-Sent Events) responses into a single blocking result.
 *
 * <p>Auth headers differ from Storage API: uses lowercase {@code x-storageapi-token}
 * and {@code x-storageapi-url} instead of {@code X-StorageApi-Token}.
 */
public class KaiClient {

    private static final Logger LOG = LoggerFactory.getLogger(KaiClient.class);

    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private static final String HEADER_TOKEN = "x-storageapi-token";
    private static final String HEADER_STORAGE_URL = "x-storageapi-url";

    private final String baseUrl;
    private final String storageToken;
    private final String storageApiUrl;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;

    /**
     * Creates a new Kai client.
     *
     * @param baseUrl       base URL of the Kai service (e.g. "https://kai.keboola.com")
     * @param storageToken  Keboola Storage API token
     * @param storageApiUrl full Storage API URL (e.g. "https://connection.keboola.com")
     */
    public KaiClient(String baseUrl, String storageToken, String storageApiUrl) {
        this.baseUrl = baseUrl;
        this.storageToken = storageToken;
        this.storageApiUrl = storageApiUrl;

        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(DriverConfig.KAI_HTTP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .readTimeout(DriverConfig.KAI_HTTP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .writeTimeout(DriverConfig.KAI_HTTP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .build();

        this.objectMapper = new ObjectMapper();
    }

    /**
     * Sends a chat message to Kai and blocks until the full response is collected.
     *
     * @param chatId    unique chat session ID (one per JDBC connection)
     * @param message   the user message text
     * @param branchId  the current branch ID as a number (may be null for DuckDB-only)
     * @return aggregated response with full text and optional extracted SQL
     * @throws KeboolaJdbcException if the request fails or Kai returns an error
     */
    public KaiResponse chat(String chatId, String message, Long branchId) throws KeboolaJdbcException {
        LOG.info("Sending Kai chat message (chatId={}, length={})", chatId, message.length());

        String requestBody = buildChatRequestBody(chatId, message, branchId);

        Request request = new Request.Builder()
                .url(baseUrl + "/api/chat")
                .header(HEADER_TOKEN, storageToken)
                .header(HEADER_STORAGE_URL, storageApiUrl)
                .header("Accept", "text/event-stream")
                .post(RequestBody.create(requestBody, JSON))
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            int code = response.code();
            if (code != 200) {
                ResponseBody body = response.body();
                String detail = body != null ? body.string() : "no body";
                throw KeboolaJdbcException.connectionFailed(
                        "Kai API returned HTTP " + code + ": " + detail);
            }

            ResponseBody body = response.body();
            if (body == null) {
                throw KeboolaJdbcException.connectionFailed("Kai API returned empty response body");
            }

            return parseSseResponse(body);
        } catch (IOException e) {
            throw KeboolaJdbcException.connectionFailed("Kai API request failed: " + e.getMessage(), e);
        }
    }

    /**
     * Health check for the Kai service.
     *
     * @return true if Kai responds to ping
     */
    public boolean ping() {
        Request request = new Request.Builder()
                .url(baseUrl + "/ping")
                .get()
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            return response.isSuccessful();
        } catch (IOException e) {
            LOG.warn("Kai ping failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Parses an SSE response body, accumulating text from "text" events.
     * Stops on "finish" or "error" events.
     *
     * <p>SSE format:
     * <pre>
     * event: text
     * data: {"text": "Hello"}
     *
     * event: finish
     * data: {}
     * </pre>
     *
     * @param body the response body to parse
     * @return aggregated KaiResponse
     */
    KaiResponse parseSseResponse(ResponseBody body) throws IOException {
        StringBuilder fullText = new StringBuilder();
        String currentEvent = null;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(body.byteStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("event:")) {
                    currentEvent = line.substring(6).trim();
                } else if (line.startsWith("data:")) {
                    String data = line.substring(5).trim();
                    if ("text".equals(currentEvent)) {
                        handleTextEvent(data, fullText);
                    } else if ("error".equals(currentEvent)) {
                        return handleErrorEvent(data);
                    } else if ("finish".equals(currentEvent)) {
                        break;
                    }
                }
                // Empty lines and other lines are ignored
            }
        }

        String text = fullText.toString();
        if (text.isEmpty()) {
            return KaiResponse.error("Kai returned an empty response");
        }
        return KaiResponse.success(text);
    }

    private void handleTextEvent(String data, StringBuilder fullText) {
        try {
            Map<?, ?> parsed = objectMapper.readValue(data, Map.class);
            Object textValue = parsed.get("text");
            if (textValue != null) {
                fullText.append(textValue.toString());
            }
        } catch (IOException e) {
            LOG.debug("Failed to parse SSE text event data: {}", data);
        }
    }

    private KaiResponse handleErrorEvent(String data) {
        try {
            Map<?, ?> parsed = objectMapper.readValue(data, Map.class);
            Object errorMsg = parsed.get("message");
            String msg = errorMsg != null ? errorMsg.toString() : "Unknown Kai error";
            return KaiResponse.error(msg);
        } catch (IOException e) {
            return KaiResponse.error("Kai returned an error (unparseable): " + data);
        }
    }

    private String buildChatRequestBody(String chatId, String message, Long branchId) {
        try {
            Map<String, Object> messagePart = new HashMap<>();
            messagePart.put("type", "text");
            messagePart.put("text", message);

            Map<String, Object> messageObj = new HashMap<>();
            messageObj.put("id", UUID.randomUUID().toString());
            messageObj.put("role", "user");
            messageObj.put("parts", new Object[]{messagePart});

            Map<String, Object> body = new HashMap<>();
            body.put("id", chatId);
            body.put("message", messageObj);
            body.put("selectedChatModel", DriverConfig.KAI_DEFAULT_MODEL);
            body.put("selectedVisibilityType", DriverConfig.KAI_VISIBILITY_TYPE);
            if (branchId != null) {
                body.put("branchId", branchId);
            }

            return objectMapper.writeValueAsString(body);
        } catch (IOException e) {
            throw new RuntimeException("Failed to build Kai request body", e);
        }
    }
}
