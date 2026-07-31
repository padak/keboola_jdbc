package com.keboola.jdbc;

import com.keboola.jdbc.auth.AuthMode;
import com.keboola.jdbc.config.DriverConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

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

    // -------------------------------------------------------------------------
    // getPropertyInfo()
    // -------------------------------------------------------------------------

    /** Reserved TLD: never resolves, so API-backed choices always fail to load. */
    private static final String UNREACHABLE_URL = "jdbc:keboola://unreachable.invalid";

    private static final String FAKE_STORAGE_TOKEN = "1234-fake-storage-token";
    private static final String FAKE_PAT           = "kbc_pat_fake-personal-access-token";

    @Test
    void getPropertyInfo_withoutProperties_describesEveryConnectionProperty() throws SQLException {
        DriverPropertyInfo[] props =
                driver.getPropertyInfo("jdbc:keboola://connection.keboola.com", null);

        assertArrayEquals(new String[] {"token", "auth", "project", "branch", "workspace"},
                Arrays.stream(props).map(p -> p.name).toArray(String[]::new));
    }

    @Test
    void getPropertyInfo_authProperty_isOptionalAndOffersBothModes() throws SQLException {
        DriverPropertyInfo auth = property(driver.getPropertyInfo(UNREACHABLE_URL, null), "auth");

        assertFalse(auth.required, "The mode is inferred from the credential when omitted");
        assertArrayEquals(
                new String[] {AuthMode.STORAGE_TOKEN.propertyValue(), AuthMode.PAT.propertyValue()},
                auth.choices);
    }

    @Test
    void getPropertyInfo_projectProperty_isOptionalAndExplainsWhenItIsNeeded() throws SQLException {
        DriverPropertyInfo project = property(driver.getPropertyInfo(UNREACHABLE_URL, null), "project");

        assertFalse(project.required, "Only a multi-project Personal Access Token needs it");
        assertTrue(project.description.contains("Personal Access Token"), project.description);
        assertEquals(0, project.choices.length, "No credential means no project lookup");
    }

    @Test
    void getPropertyInfo_tokenProperty_isRequired() throws SQLException {
        DriverPropertyInfo token = property(driver.getPropertyInfo(UNREACHABLE_URL, null), "token");

        assertTrue(token.required);
    }

    @Test
    void getPropertyInfo_credentialSuppliedAsPassword_isEchoedAsToken() throws SQLException {
        Properties info = new Properties();
        info.setProperty("password", FAKE_STORAGE_TOKEN);

        DriverPropertyInfo[] props = driver.getPropertyInfo(UNREACHABLE_URL, info);

        assertEquals(FAKE_STORAGE_TOKEN, property(props, "token").value);
    }

    @Test
    void getPropertyInfo_storageTokenAndUnreachableApi_returnsEmptyChoicesInsteadOfThrowing()
            throws SQLException {
        Properties info = new Properties();
        info.setProperty("token", FAKE_STORAGE_TOKEN);

        DriverPropertyInfo[] props = driver.getPropertyInfo(UNREACHABLE_URL, info);

        assertEquals(0, property(props, "branch").choices.length);
        assertEquals(0, property(props, "workspace").choices.length);
    }

    @Test
    void getPropertyInfo_patAndUnreachableApi_returnsEmptyChoicesInsteadOfThrowing()
            throws SQLException {
        Properties info = new Properties();
        info.setProperty("token", FAKE_PAT);

        DriverPropertyInfo[] props = driver.getPropertyInfo(UNREACHABLE_URL, info);

        assertEquals(0, property(props, "project").choices.length);
        assertEquals(0, property(props, "branch").choices.length);
        assertEquals(0, property(props, "workspace").choices.length);
    }

    @Test
    void getPropertyInfo_echoesSuppliedValues() throws SQLException {
        Properties info = new Properties();
        info.setProperty("token", FAKE_PAT);
        info.setProperty("auth", "pat");
        info.setProperty("project", "1234");
        info.setProperty("branch", "5678");
        info.setProperty("workspace", "9012");

        DriverPropertyInfo[] props = driver.getPropertyInfo(UNREACHABLE_URL, info);

        assertEquals(FAKE_PAT, property(props, "token").value);
        assertEquals("pat", property(props, "auth").value);
        assertEquals("1234", property(props, "project").value);
        assertEquals("5678", property(props, "branch").value);
        assertEquals("9012", property(props, "workspace").value);
    }

    private static DriverPropertyInfo property(DriverPropertyInfo[] props, String name) {
        return Arrays.stream(props)
                .filter(p -> name.equals(p.name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No such driver property: " + name));
    }
}
