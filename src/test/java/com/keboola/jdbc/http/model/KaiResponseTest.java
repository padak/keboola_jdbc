package com.keboola.jdbc.http.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link KaiResponse} SQL extraction logic.
 */
class KaiResponseTest {

    @Test
    void extractSqlFromMarkdown_singleBlock() {
        String text = "Here's your query:\n```sql\nSELECT * FROM users\n```\nDone.";
        assertEquals("SELECT * FROM users", KaiResponse.extractSqlFromMarkdown(text));
    }

    @Test
    void extractSqlFromMarkdown_multipleBlocks() {
        String text = "First:\n```sql\nSELECT 1\n```\nSecond:\n```sql\nSELECT 2\n```";
        assertEquals("SELECT 1;\nSELECT 2", KaiResponse.extractSqlFromMarkdown(text));
    }

    @Test
    void extractSqlFromMarkdown_noBlocks() {
        assertNull(KaiResponse.extractSqlFromMarkdown("No SQL here"));
    }

    @Test
    void extractSqlFromMarkdown_nullInput() {
        assertNull(KaiResponse.extractSqlFromMarkdown(null));
    }

    @Test
    void extractSqlFromMarkdown_emptyInput() {
        assertNull(KaiResponse.extractSqlFromMarkdown(""));
    }

    @Test
    void extractSqlFromMarkdown_emptyBlock() {
        assertNull(KaiResponse.extractSqlFromMarkdown("```sql\n```"));
    }

    @Test
    void extractSqlFromMarkdown_caseInsensitive() {
        String text = "```SQL\nSELECT 1\n```";
        assertEquals("SELECT 1", KaiResponse.extractSqlFromMarkdown(text));
    }

    @Test
    void extractSqlFromMarkdown_multilineQuery() {
        String text = "```sql\nSELECT\n  id,\n  name\nFROM users\nWHERE id > 10\n```";
        assertEquals("SELECT\n  id,\n  name\nFROM users\nWHERE id > 10",
                KaiResponse.extractSqlFromMarkdown(text));
    }

    @Test
    void success_extractsSql() {
        KaiResponse resp = KaiResponse.success("Here: ```sql\nSELECT 1\n```");
        assertFalse(resp.hasError());
        assertEquals("SELECT 1", resp.getExtractedSql());
        assertNotNull(resp.getFullText());
    }

    @Test
    void error_setsFields() {
        KaiResponse resp = KaiResponse.error("bad request");
        assertTrue(resp.hasError());
        assertEquals("bad request", resp.getErrorMessage());
        assertNull(resp.getFullText());
        assertNull(resp.getExtractedSql());
    }
}
