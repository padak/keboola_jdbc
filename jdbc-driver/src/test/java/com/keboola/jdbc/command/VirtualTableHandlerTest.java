package com.keboola.jdbc.command;

import com.keboola.jdbc.config.DriverConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for VirtualTableHandler - verifies pattern matching and LIMIT parsing.
 */
class VirtualTableHandlerTest {

    private final VirtualTableHandler handler = new VirtualTableHandler();

    // -------------------------------------------------------------------------
    // canHandle - positive cases
    // -------------------------------------------------------------------------

    @Test
    void canHandle_selectFromComponents_returnsTrue() {
        assertTrue(handler.canHandle("SELECT * FROM _keboola.components"));
    }

    @Test
    void canHandle_lowerCaseSelectFromEvents_returnsTrue() {
        assertTrue(handler.canHandle("select * from _keboola.events LIMIT 5"));
    }

    @Test
    void canHandle_selectFromJobs_returnsTrue() {
        assertTrue(handler.canHandle("SELECT * FROM _keboola.jobs"));
    }

    @Test
    void canHandle_selectFromTables_returnsTrue() {
        assertTrue(handler.canHandle("SELECT * FROM _keboola.tables"));
    }

    @Test
    void canHandle_selectFromBuckets_returnsTrue() {
        assertTrue(handler.canHandle("SELECT * FROM _keboola.buckets"));
    }

    // -------------------------------------------------------------------------
    // canHandle - negative cases
    // -------------------------------------------------------------------------

    @Test
    void canHandle_plainSelect_returnsFalse() {
        assertFalse(handler.canHandle("SELECT 1"));
    }

    @Test
    void canHandle_keboolaHelp_returnsFalse() {
        assertFalse(handler.canHandle("KEBOOLA HELP"));
    }

    @Test
    void canHandle_regularTable_returnsFalse() {
        assertFalse(handler.canHandle("SELECT * FROM my_table"));
    }

    // -------------------------------------------------------------------------
    // parseLimit - extracts correct value
    // -------------------------------------------------------------------------

    @Test
    void parseLimit_withExplicitLimit_returnsValue() {
        int limit = VirtualTableHandler.parseLimit("SELECT * FROM _keboola.events LIMIT 10");
        assertEquals(10, limit);
    }

    @Test
    void parseLimit_withLargeLimit_returnsValue() {
        int limit = VirtualTableHandler.parseLimit("SELECT * FROM _keboola.jobs LIMIT 500");
        assertEquals(500, limit);
    }

    @Test
    void parseLimit_lowerCaseLimit_returnsValue() {
        int limit = VirtualTableHandler.parseLimit("select * from _keboola.events limit 25");
        assertEquals(25, limit);
    }

    // -------------------------------------------------------------------------
    // parseLimit - returns default when no LIMIT
    // -------------------------------------------------------------------------

    @Test
    void parseLimit_noLimitClause_returnsDefault() {
        int limit = VirtualTableHandler.parseLimit("SELECT * FROM _keboola.events");
        assertEquals(DriverConfig.VIRTUAL_TABLE_DEFAULT_LIMIT, limit);
    }

    @Test
    void parseLimit_noLimitClause_returnsOneHundred() {
        int limit = VirtualTableHandler.parseLimit("SELECT * FROM _keboola.components");
        assertEquals(100, limit);
    }
}
