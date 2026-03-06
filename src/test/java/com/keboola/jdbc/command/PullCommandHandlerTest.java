package com.keboola.jdbc.command;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PullCommandHandler pattern matching.
 * Data transfer tests are in PullCommandIT (requires real backends).
 */
class PullCommandHandlerTest {

    private PullCommandHandler handler;

    @BeforeEach
    void setUp() {
        handler = new PullCommandHandler();
    }

    // --- canHandle: PULL TABLE ---

    @Test
    void canHandle_pullTableThreeParts() {
        assertTrue(handler.canHandle("KEBOOLA PULL TABLE in.c-main.orders"));
    }

    @Test
    void canHandle_pullTableWithAlias() {
        assertTrue(handler.canHandle("KEBOOLA PULL TABLE in.c-main.orders INTO my_orders"));
    }

    @Test
    void canHandle_pullTableCaseInsensitive() {
        assertTrue(handler.canHandle("keboola pull table In.C-Main.Orders"));
    }

    @Test
    void canHandle_pullTableWithQuotedParts() {
        assertTrue(handler.canHandle("KEBOOLA PULL TABLE \"in\".\"c-main\".\"orders\""));
    }

    @Test
    void canHandle_pullTableWithSemicolon() {
        assertTrue(handler.canHandle("KEBOOLA PULL TABLE in.c-main.orders;"));
    }

    @Test
    void canHandle_pullTableWithQuotedAlias() {
        assertTrue(handler.canHandle("KEBOOLA PULL TABLE in.c-main.orders INTO \"my_orders\""));
    }

    // --- canHandle: PULL QUERY ---

    @Test
    void canHandle_pullQuery() {
        assertTrue(handler.canHandle("KEBOOLA PULL QUERY SELECT * FROM my_table INTO local_copy"));
    }

    @Test
    void canHandle_pullQueryComplex() {
        assertTrue(handler.canHandle(
                "KEBOOLA PULL QUERY SELECT id, name FROM \"in.c-main\".\"orders\" WHERE status = 'active' INTO active_orders"));
    }

    @Test
    void canHandle_pullQueryCaseInsensitive() {
        assertTrue(handler.canHandle("keboola pull query SELECT 1 INTO result"));
    }

    @Test
    void canHandle_pullQueryWithSemicolon() {
        assertTrue(handler.canHandle("KEBOOLA PULL QUERY SELECT 1 INTO result;"));
    }

    // --- canHandle: rejects ---

    @Test
    void canHandle_rejectsNull() {
        assertFalse(handler.canHandle(null));
    }

    @Test
    void canHandle_rejectsRegularSql() {
        assertFalse(handler.canHandle("SELECT * FROM orders"));
    }

    @Test
    void canHandle_rejectsKeboolaHelp() {
        assertFalse(handler.canHandle("KEBOOLA HELP"));
    }

    @Test
    void canHandle_rejectsBackendSwitch() {
        assertFalse(handler.canHandle("KEBOOLA USE BACKEND duckdb"));
    }

    @Test
    void canHandle_rejectsPullQueryWithoutAlias() {
        assertFalse(handler.canHandle("KEBOOLA PULL QUERY SELECT * FROM orders"));
    }

    @Test
    void canHandle_rejectsPullTableTwoParts() {
        // Must have 3 parts: stage.bucket.table
        assertFalse(handler.canHandle("KEBOOLA PULL TABLE orders"));
    }
}
