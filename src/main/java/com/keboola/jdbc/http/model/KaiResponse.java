package com.keboola.jdbc.http.model;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents the aggregated response from a Kai AI chat session.
 * Collects all SSE text events into a single response and optionally extracts SQL from markdown.
 */
public class KaiResponse {

    private static final Pattern SQL_BLOCK_PATTERN = Pattern.compile(
            "```sql\\s*\\n(.*?)```",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE
    );

    private final String fullText;
    private final String extractedSql;
    private final boolean hasError;
    private final String errorMessage;

    public KaiResponse(String fullText, String extractedSql, boolean hasError, String errorMessage) {
        this.fullText = fullText;
        this.extractedSql = extractedSql;
        this.hasError = hasError;
        this.errorMessage = errorMessage;
    }

    /**
     * Creates a successful response with optional SQL extraction from markdown.
     */
    public static KaiResponse success(String fullText) {
        String sql = extractSqlFromMarkdown(fullText);
        return new KaiResponse(fullText, sql, false, null);
    }

    /**
     * Creates an error response.
     */
    public static KaiResponse error(String errorMessage) {
        return new KaiResponse(null, null, true, errorMessage);
    }

    /**
     * Extracts SQL from markdown ```sql ... ``` code blocks.
     * If multiple blocks exist, joins them with semicolons.
     *
     * @param text the markdown text to parse
     * @return extracted SQL, or null if no SQL blocks found
     */
    public static String extractSqlFromMarkdown(String text) {
        if (text == null || text.isEmpty()) return null;

        Matcher matcher = SQL_BLOCK_PATTERN.matcher(text);
        List<String> blocks = new ArrayList<>();
        while (matcher.find()) {
            String sql = matcher.group(1).trim();
            if (!sql.isEmpty()) {
                blocks.add(sql);
            }
        }

        if (blocks.isEmpty()) return null;
        return String.join(";\n", blocks);
    }

    public String getFullText() { return fullText; }
    public String getExtractedSql() { return extractedSql; }
    public boolean hasError() { return hasError; }
    public String getErrorMessage() { return errorMessage; }
}
