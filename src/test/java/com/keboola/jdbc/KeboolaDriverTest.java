package com.keboola.jdbc;

import com.keboola.jdbc.config.DriverConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KeboolaDriver - verifies URL acceptance, version reporting, and compliance flag.
 */
class KeboolaDriverTest {

    private KeboolaDriver driver;

    @BeforeEach
    void setUp() {
        driver = new KeboolaDriver();
    }

    // -------------------------------------------------------------------------
    // acceptsURL()
    // -------------------------------------------------------------------------

    @Test
    void acceptsURL_validKeboolaUrl_returnsTrue() throws SQLException {
        assertTrue(driver.acceptsURL("jdbc:keboola://connection.keboola.com"));
    }

    @Test
    void acceptsURL_validKeboolaUrlWithEuHost_returnsTrue() throws SQLException {
        assertTrue(driver.acceptsURL("jdbc:keboola://connection.eu-central-1.keboola.com"));
    }

    @Test
    void acceptsURL_mysqlUrl_returnsFalse() throws SQLException {
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost:3306/mydb"));
    }

    @Test
    void acceptsURL_postgresUrl_returnsFalse() throws SQLException {
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432/mydb"));
    }

    @Test
    void acceptsURL_snowflakeUrl_returnsFalse() throws SQLException {
        assertFalse(driver.acceptsURL("jdbc:snowflake://account.snowflakecomputing.com"));
    }

    @Test
    void acceptsURL_null_returnsFalse() throws SQLException {
        assertFalse(driver.acceptsURL(null));
    }

    @Test
    void acceptsURL_emptyString_returnsFalse() throws SQLException {
        assertFalse(driver.acceptsURL(""));
    }

    @Test
    void acceptsURL_partialPrefix_returnsFalse() throws SQLException {
        assertFalse(driver.acceptsURL("jdbc:keboola:connection.keboola.com"));
    }

    // -------------------------------------------------------------------------
    // getMajorVersion() and getMinorVersion()
    // -------------------------------------------------------------------------

    @Test
    void getMajorVersion_returnsConfiguredMajorVersion() {
        assertEquals(DriverConfig.MAJOR_VERSION, driver.getMajorVersion());
    }

    @Test
    void getMinorVersion_returnsConfiguredMinorVersion() {
        assertEquals(DriverConfig.MINOR_VERSION, driver.getMinorVersion());
    }

    @Test
    void getMajorVersion_isPositive() {
        assertTrue(driver.getMajorVersion() > 0, "Major version must be a positive integer");
    }

    // -------------------------------------------------------------------------
    // jdbcCompliant()
    // -------------------------------------------------------------------------

    @Test
    void jdbcCompliant_returnsFalse() {
        // Keboola driver is read-only and does not implement full JDBC spec
        assertFalse(driver.jdbcCompliant());
    }

    // -------------------------------------------------------------------------
    // connect() - returns null for non-Keboola URLs
    // -------------------------------------------------------------------------

    @Test
    void connect_nonKeboolaUrl_returnsNull() throws SQLException {
        // Per JDBC spec, driver must return null if it does not handle the URL
        assertNull(driver.connect("jdbc:mysql://localhost/db", null));
    }
}
