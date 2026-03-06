package com.keboola.jdbc.command;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SessionLogHandler} command matching.
 */
class SessionLogHandlerTest {

    private SessionLogHandler handler;

    @BeforeEach
    void setUp() {
        handler = new SessionLogHandler();
    }

    @Test
    void canHandle_matchesExactCommand() {
        assertTrue(handler.canHandle("KEBOOLA SESSION LOG"));
    }

    @Test
    void canHandle_caseInsensitive() {
        assertTrue(handler.canHandle("keboola session log"));
        assertTrue(handler.canHandle("Keboola Session Log"));
        assertTrue(handler.canHandle("KEBOOLA session LOG"));
    }

    @Test
    void canHandle_withSemicolon() {
        assertTrue(handler.canHandle("KEBOOLA SESSION LOG;"));
        assertTrue(handler.canHandle("KEBOOLA SESSION LOG ;"));
        assertTrue(handler.canHandle("KEBOOLA SESSION LOG ; "));
    }

    @Test
    void canHandle_withWhitespace() {
        assertTrue(handler.canHandle("  KEBOOLA SESSION LOG  "));
        assertTrue(handler.canHandle("  KEBOOLA  SESSION  LOG  ;  "));
    }

    @Test
    void canHandle_rejectsNull() {
        assertFalse(handler.canHandle(null));
    }

    @Test
    void canHandle_rejectsOtherCommands() {
        assertFalse(handler.canHandle("SELECT 1"));
        assertFalse(handler.canHandle("KEBOOLA HELP"));
        assertFalse(handler.canHandle("KEBOOLA SESSION"));
        assertFalse(handler.canHandle("KEBOOLA LOG"));
        assertFalse(handler.canHandle("SESSION LOG"));
    }
}
