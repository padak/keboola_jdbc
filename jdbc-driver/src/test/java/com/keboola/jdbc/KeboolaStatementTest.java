package com.keboola.jdbc;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KeboolaStatement helper methods (splitStatements, etc.).
 */
class KeboolaStatementTest {

    // -------------------------------------------------------------------------
    // splitStatements
    // -------------------------------------------------------------------------

    @Test
    void splitStatements_singleStatement() {
        List<String> result = KeboolaStatement.splitStatements("SELECT 1");
        assertEquals(List.of("SELECT 1"), result);
    }

    @Test
    void splitStatements_singleWithTrailingSemicolon() {
        List<String> result = KeboolaStatement.splitStatements("SELECT 1;");
        assertEquals(List.of("SELECT 1"), result);
    }

    @Test
    void splitStatements_twoStatements() {
        List<String> result = KeboolaStatement.splitStatements("SET TEST=1; SELECT $TEST");
        assertEquals(List.of("SET TEST=1", "SELECT $TEST"), result);
    }

    @Test
    void splitStatements_multipleWithNewlines() {
        List<String> result = KeboolaStatement.splitStatements("SET TEST=1;\nSELECT $TEST;");
        assertEquals(List.of("SET TEST=1", "SELECT $TEST"), result);
    }

    @Test
    void splitStatements_preservesQuotedSemicolons() {
        List<String> result = KeboolaStatement.splitStatements(
                "SELECT * FROM \"schema;name\".\"table\"");
        assertEquals(1, result.size());
        assertEquals("SELECT * FROM \"schema;name\".\"table\"", result.get(0));
    }

    @Test
    void splitStatements_preservesSingleQuotedSemicolons() {
        List<String> result = KeboolaStatement.splitStatements(
                "SELECT 'hello;world' AS greeting; SELECT 2");
        assertEquals(2, result.size());
        assertEquals("SELECT 'hello;world' AS greeting", result.get(0));
        assertEquals("SELECT 2", result.get(1));
    }

    @Test
    void splitStatements_emptyBetweenSemicolons() {
        List<String> result = KeboolaStatement.splitStatements("SELECT 1;; SELECT 2");
        assertEquals(List.of("SELECT 1", "SELECT 2"), result);
    }

    @Test
    void splitStatements_emptyInput() {
        List<String> result = KeboolaStatement.splitStatements("");
        assertTrue(result.isEmpty());
    }

    @Test
    void splitStatements_onlySemicolons() {
        List<String> result = KeboolaStatement.splitStatements(";;;");
        assertTrue(result.isEmpty());
    }

    @Test
    void splitStatements_threeStatements() {
        List<String> result = KeboolaStatement.splitStatements(
                "SET X=1; SET Y=2; SELECT $X + $Y");
        assertEquals(3, result.size());
        assertEquals("SET X=1", result.get(0));
        assertEquals("SET Y=2", result.get(1));
        assertEquals("SELECT $X + $Y", result.get(2));
    }

    @Test
    void splitStatements_useSchemaAndSelect() {
        List<String> result = KeboolaStatement.splitStatements(
                "USE SCHEMA \"in.c-main\"; SELECT * FROM \"my_table\" LIMIT 10");
        assertEquals(2, result.size());
        assertEquals("USE SCHEMA \"in.c-main\"", result.get(0));
        assertEquals("SELECT * FROM \"my_table\" LIMIT 10", result.get(1));
    }
}
