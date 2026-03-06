package com.keboola.jdbc.command;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link PushCommandHandler#canHandle(String)}.
 */
class PushCommandHandlerTest {

    private PushCommandHandler handler;

    @BeforeEach
    void setUp() {
        handler = new PushCommandHandler();
    }

    @Test
    void canHandle_simplePush() {
        assertTrue(handler.canHandle("KEBOOLA PUSH TABLE my_data"));
    }

    @Test
    void canHandle_pushWithInto() {
        assertTrue(handler.canHandle("KEBOOLA PUSH TABLE my_data INTO in.c-main.target"));
    }

    @Test
    void canHandle_caseInsensitive() {
        assertTrue(handler.canHandle("keboola push table my_data"));
        assertTrue(handler.canHandle("Keboola Push Table my_data INTO in.c-main.target"));
    }

    @Test
    void canHandle_withSemicolon() {
        assertTrue(handler.canHandle("KEBOOLA PUSH TABLE my_data;"));
        assertTrue(handler.canHandle("KEBOOLA PUSH TABLE my_data INTO in.c-main.target;"));
    }

    @Test
    void canHandle_withQuotedNames() {
        assertTrue(handler.canHandle("KEBOOLA PUSH TABLE \"my_data\""));
        assertTrue(handler.canHandle("KEBOOLA PUSH TABLE \"my_data\" INTO \"in\".\"c-main\".\"target\""));
    }

    @Test
    void canHandle_rejectsNull() {
        assertFalse(handler.canHandle(null));
    }

    @Test
    void canHandle_rejectsRegularSql() {
        assertFalse(handler.canHandle("SELECT * FROM my_table"));
        assertFalse(handler.canHandle("INSERT INTO foo VALUES (1)"));
        assertFalse(handler.canHandle("DROP TABLE my_data"));
    }

    @Test
    void canHandle_rejectsPullCommand() {
        assertFalse(handler.canHandle("KEBOOLA PULL TABLE in.c-main.orders"));
        assertFalse(handler.canHandle("KEBOOLA PULL QUERY SELECT 1 INTO test"));
    }

    @Test
    void canHandle_withLeadingAndTrailingWhitespace() {
        assertTrue(handler.canHandle("  KEBOOLA PUSH TABLE my_data  "));
        assertTrue(handler.canHandle("  KEBOOLA PUSH TABLE my_data INTO in.c-main.target  ;  "));
    }
}
