package com.keboola.jdbc;

import com.keboola.jdbc.auth.AuthMode;
import com.keboola.jdbc.auth.AuthProvider;
import com.keboola.jdbc.auth.PatAuthProvider;
import com.keboola.jdbc.auth.StorageTokenAuthProvider;
import com.keboola.jdbc.config.ConnectionConfig;
import com.keboola.jdbc.exception.KeboolaJdbcException;
import com.keboola.jdbc.http.ProgrammaticAuthClient;
import com.keboola.jdbc.http.QueryServiceClient;
import com.keboola.jdbc.http.StorageApiClient;
import com.keboola.jdbc.http.model.JobStatus;
import com.keboola.jdbc.http.model.PatInfo;
import com.keboola.jdbc.http.model.QueryJob;
import com.keboola.jdbc.http.model.QueryResult;
import com.keboola.jdbc.http.model.ResultColumn;
import com.keboola.jdbc.http.model.StatementStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KeboolaConnectionTest {

    @Mock
    private StorageApiClient storageClient;

    @Mock
    private QueryServiceClient queryClient;

    private KeboolaConnection conn;

    @BeforeEach
    void setUp() throws Exception {
        conn = new KeboolaConnection(
                "test-host", storageClient, queryClient,
                /* branchId */ 100L, /* workspaceId */ 200L,
                /* catalog */ "MY_DB", /* schema */ "MY_SCHEMA");

        // Lenient stubs so individual tests that don't issue SQL don't fail on unused-stub.
        // Tests that need different behavior override these.
        StatementStatus stmtStatus = new StatementStatus(
                "stmt-1", "USE SCHEMA \"X\"", "completed", null, 0, 0);
        JobStatus jobStatus = new JobStatus("job-1", "completed",
                Collections.singletonList(stmtStatus));
        QueryResult emptyResult = new QueryResult("completed",
                Collections.<ResultColumn>emptyList(),
                Collections.<List<String>>emptyList(), 0, 0);

        lenient().when(queryClient.submitJob(anyLong(), anyLong(), any(), anyString()))
                .thenReturn(new QueryJob("job-1", "test-session"));
        lenient().when(queryClient.submitJob(anyLong(), anyLong(), any()))
                .thenReturn(new QueryJob("job-1", "test-session"));
        lenient().when(queryClient.waitForCompletion(anyString())).thenReturn(jobStatus);
        lenient().when(queryClient.fetchResults(anyString(), anyString(), anyInt(), anyInt()))
                .thenReturn(emptyResult);
    }

    // ---------------------------------------------------------------------
    // Catalog & schema accessors
    // ---------------------------------------------------------------------

    @Test
    void getCatalog_returnsInjectedValue() throws SQLException {
        assertEquals("MY_DB", conn.getCatalog());
    }

    @Test
    void getSchema_returnsInjectedValue() throws SQLException {
        assertEquals("MY_SCHEMA", conn.getSchema());
    }

    @Test
    void setCatalog_issuesUseDatabaseStatement() throws Exception {
        conn.setCatalog("OTHER_DB");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> stmtCaptor = ArgumentCaptor.forClass(List.class);
        org.mockito.Mockito.verify(queryClient).submitJob(
                anyLong(), anyLong(), stmtCaptor.capture(), anyString());
        List<String> sentStatements = stmtCaptor.getValue();
        assertEquals(1, sentStatements.size());
        assertEquals("USE DATABASE \"OTHER_DB\"", sentStatements.get(0));
        assertEquals("OTHER_DB", conn.getCatalog());
    }

    @Test
    void setCatalog_escapesEmbeddedQuotes() throws Exception {
        conn.setCatalog("db\"with\"quotes");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> stmtCaptor = ArgumentCaptor.forClass(List.class);
        org.mockito.Mockito.verify(queryClient).submitJob(
                anyLong(), anyLong(), stmtCaptor.capture(), anyString());
        assertTrue(stmtCaptor.getValue().get(0).contains("db\"\"with\"\"quotes"),
                "Embedded quotes must be doubled");
    }

    @Test
    void setCatalog_null_isIgnored() throws Exception {
        conn.setCatalog(null);

        // catalog field remains unchanged
        assertEquals("MY_DB", conn.getCatalog());
        org.mockito.Mockito.verify(queryClient,
                org.mockito.Mockito.never()).submitJob(anyLong(), anyLong(), any(), anyString());
    }

    @Test
    void setSchema_issuesUseSchemaStatement() throws Exception {
        conn.setSchema("OTHER_SCHEMA");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> stmtCaptor = ArgumentCaptor.forClass(List.class);
        org.mockito.Mockito.verify(queryClient).submitJob(
                anyLong(), anyLong(), stmtCaptor.capture(), anyString());
        assertEquals("USE SCHEMA \"OTHER_SCHEMA\"", stmtCaptor.getValue().get(0));
        assertEquals("OTHER_SCHEMA", conn.getSchema());
    }

    @Test
    void setSchema_null_clearsSchemaWithoutIssuingSql() throws Exception {
        conn.setSchema(null);

        assertNull(conn.getSchema());
        org.mockito.Mockito.verify(queryClient,
                org.mockito.Mockito.never()).submitJob(anyLong(), anyLong(), any(), anyString());
    }

    @Test
    void updateCatalogFromServer_setsCatalogWithoutSql() {
        conn.updateCatalogFromServer("FROM_SERVER_DB");

        try {
            assertEquals("FROM_SERVER_DB", conn.getCatalog());
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void updateSchemaFromServer_setsSchemaWithoutSql() throws SQLException {
        conn.updateSchemaFromServer("FROM_SERVER_SCHEMA");

        assertEquals("FROM_SERVER_SCHEMA", conn.getSchema());
    }

    // ---------------------------------------------------------------------
    // Lifecycle: close, isClosed, isValid
    // ---------------------------------------------------------------------

    @Test
    void isClosed_initially_false() throws SQLException {
        assertFalse(conn.isClosed());
    }

    @Test
    void close_setsIsClosedTrue() throws SQLException {
        conn.close();
        assertTrue(conn.isClosed());
    }

    @Test
    void close_calledTwice_isIdempotent() throws SQLException {
        conn.close();
        conn.close();
        assertTrue(conn.isClosed());
    }

    @Test
    void isValid_whenOpen_returnsTrue() throws SQLException {
        assertTrue(conn.isValid(0));
    }

    @Test
    void isValid_whenClosed_returnsFalse() throws SQLException {
        conn.close();
        assertFalse(conn.isValid(0));
    }

    @Test
    void getCatalog_afterClose_throwsSQLException() throws SQLException {
        conn.close();
        assertThrows(SQLException.class, conn::getCatalog);
    }

    @Test
    void setCatalog_afterClose_throwsSQLException() throws SQLException {
        conn.close();
        assertThrows(SQLException.class, () -> conn.setCatalog("x"));
    }

    @Test
    void setSchema_afterClose_throwsSQLException() throws SQLException {
        conn.close();
        assertThrows(SQLException.class, () -> conn.setSchema("x"));
    }

    @Test
    void createStatement_afterClose_throwsSQLException() throws SQLException {
        conn.close();
        assertThrows(SQLException.class, conn::createStatement);
    }

    @Test
    void getMetaData_afterClose_throwsSQLException() throws SQLException {
        conn.close();
        assertThrows(SQLException.class, conn::getMetaData);
    }

    // ---------------------------------------------------------------------
    // Factory methods
    // ---------------------------------------------------------------------

    @Test
    void createStatement_returnsNonNullStatement() throws SQLException {
        Statement stmt = conn.createStatement();
        assertNotNull(stmt);
    }

    @Test
    void prepareStatement_returnsNonNullPreparedStatement() throws SQLException {
        PreparedStatement ps = conn.prepareStatement("SELECT 1");
        assertNotNull(ps);
    }

    @Test
    void getMetaData_returnsNonNullMetadata() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        assertNotNull(meta);
    }

    // ---------------------------------------------------------------------
    // Auto-commit, read-only, transaction isolation
    // ---------------------------------------------------------------------

    @Test
    void getAutoCommit_default_isTrue() throws SQLException {
        assertTrue(conn.getAutoCommit());
    }

    @Test
    void setAutoCommit_storesValue() throws SQLException {
        conn.setAutoCommit(false);
        assertFalse(conn.getAutoCommit());
    }

    @Test
    void isReadOnly_default_isTrue() throws SQLException {
        assertTrue(conn.isReadOnly());
    }

    @Test
    void getTransactionIsolation_returnsReadCommitted() throws SQLException {
        // Driver reports TRANSACTION_READ_COMMITTED — the Snowflake backend default.
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, conn.getTransactionIsolation());
    }

    @Test
    void commit_isNoOpForOpenConnection() throws SQLException {
        conn.commit(); // must not throw
    }

    @Test
    void rollback_isNoOpForOpenConnection() throws SQLException {
        conn.rollback(); // must not throw
    }

    @Test
    void rollback_withSavepoint_throwsUnsupported() {
        assertThrows(SQLFeatureNotSupportedException.class, () -> conn.rollback(null));
    }

    @Test
    void commit_afterClose_throwsSQLException() throws SQLException {
        conn.close();
        assertThrows(SQLException.class, conn::commit);
    }

    @Test
    void rollback_afterClose_throwsSQLException() throws SQLException {
        conn.close();
        assertThrows(SQLException.class, conn::rollback);
    }

    // ---------------------------------------------------------------------
    // Warnings
    // ---------------------------------------------------------------------

    @Test
    void getWarnings_initiallyReturnsNull() throws SQLException {
        assertNull(conn.getWarnings());
    }

    @Test
    void clearWarnings_doesNotThrow() throws SQLException {
        conn.clearWarnings();
    }

    // ---------------------------------------------------------------------
    // setCatalog/setSchema — multiple stmt count
    // ---------------------------------------------------------------------

    @Test
    void setSchema_calledMultipleTimes_emitsOneStatementPerCall() throws Exception {
        conn.setSchema("S1");
        conn.setSchema("S2");

        org.mockito.Mockito.verify(queryClient,
                org.mockito.Mockito.times(2))
                .submitJob(anyLong(), anyLong(),
                        org.mockito.ArgumentMatchers.<List<String>>any(), anyString());
        assertEquals("S2", conn.getSchema());
    }

    // ---------------------------------------------------------------------
    // Authentication provider resolution
    // ---------------------------------------------------------------------

    private static final String FAKE_STORAGE_TOKEN = "1234-fake-storage-token";
    private static final String FAKE_PAT           = "kbc_pat_fake-personal-access-token";

    @Test
    void resolveAuthProvider_storageToken_sendsStorageTokenHeader() throws Exception {
        AuthProvider provider = KeboolaConnection.resolveAuthProvider(
                config(FAKE_STORAGE_TOKEN), noDiscoveryExpected());

        assertEquals(AuthMode.STORAGE_TOKEN, provider.mode());
        assertEquals(FAKE_STORAGE_TOKEN,
                provider.authHeaders().get(StorageTokenAuthProvider.HEADER_STORAGE_TOKEN));
    }

    @Test
    void resolveAuthProvider_patWithConfiguredProject_doesNotCallDiscovery() throws Exception {
        AuthProvider provider = KeboolaConnection.resolveAuthProvider(
                config(FAKE_PAT, "project", "1234"), noDiscoveryExpected());

        assertEquals(AuthMode.PAT, provider.mode());
        assertEquals(1234L, ((PatAuthProvider) provider).getProjectId());
        assertEquals("1234", provider.authHeaders().get(PatAuthProvider.HEADER_PROJECT_ID));
    }

    @Test
    void resolveAuthProvider_patWithOneAccessibleProject_autoSelectsIt() throws Exception {
        List<PatInfo> pats = Collections.singletonList(
                patWithProjects(new PatInfo.ProjectAccess(4321L, "Only Project", "admin")));
        ProgrammaticAuthClient authClient = mock(ProgrammaticAuthClient.class);
        when(authClient.listPersonalAccessTokens()).thenReturn(pats);

        AuthProvider provider = KeboolaConnection.resolveAuthProvider(
                config(FAKE_PAT), () -> authClient);

        assertEquals(AuthMode.PAT, provider.mode());
        assertEquals(4321L, ((PatAuthProvider) provider).getProjectId());
    }

    @Test
    void resolveAuthProvider_patProjectsRepeatedAcrossItems_countAsOneProject() throws Exception {
        // Derived tokens appear alongside the caller in the listing and can only ever
        // reach a subset of its projects, so repeated entries must not look ambiguous.
        List<PatInfo> pats = Arrays.asList(
                patWithProjects(new PatInfo.ProjectAccess(77L, "Shared Project", "admin")),
                patWithProjects(new PatInfo.ProjectAccess(77L, "Shared Project", "guest")));
        ProgrammaticAuthClient authClient = mock(ProgrammaticAuthClient.class);
        when(authClient.listPersonalAccessTokens()).thenReturn(pats);

        AuthProvider provider = KeboolaConnection.resolveAuthProvider(
                config(FAKE_PAT), () -> authClient);

        assertEquals(77L, ((PatAuthProvider) provider).getProjectId());
    }

    @Test
    void resolveAuthProvider_patWithSeveralAccessibleProjects_failsListingThem() throws Exception {
        List<PatInfo> pats = Collections.singletonList(patWithProjects(
                new PatInfo.ProjectAccess(1234L, "First Project", "admin"),
                new PatInfo.ProjectAccess(5678L, "Second Project", "guest")));
        ProgrammaticAuthClient authClient = mock(ProgrammaticAuthClient.class);
        when(authClient.listPersonalAccessTokens()).thenReturn(pats);

        KeboolaJdbcException e = assertThrows(KeboolaJdbcException.class,
                () -> KeboolaConnection.resolveAuthProvider(config(FAKE_PAT), () -> authClient));

        assertTrue(e.getMessage().contains("1234 (First Project)"), e.getMessage());
        assertTrue(e.getMessage().contains("5678 (Second Project)"), e.getMessage());
        assertTrue(e.getMessage().contains("'project'"),
                "The message must name the property that resolves the ambiguity");
        assertFalse(e.getMessage().contains(FAKE_PAT), "The credential must never be echoed");
    }

    @Test
    void resolveAuthProvider_patWithNoAccessibleProject_failsAuthentication() throws Exception {
        List<PatInfo> pats = Collections.singletonList(patWithProjects());
        ProgrammaticAuthClient authClient = mock(ProgrammaticAuthClient.class);
        when(authClient.listPersonalAccessTokens()).thenReturn(pats);

        KeboolaJdbcException e = assertThrows(KeboolaJdbcException.class,
                () -> KeboolaConnection.resolveAuthProvider(config(FAKE_PAT), () -> authClient));

        assertEquals("28000", e.getSQLState());
        assertTrue(e.getMessage().contains("does not grant access to any Keboola project"),
                e.getMessage());
        assertFalse(e.getMessage().contains(FAKE_PAT), "The credential must never be echoed");
    }

    @Test
    void resolveAuthProvider_patListingEmpty_failsAuthentication() throws Exception {
        ProgrammaticAuthClient authClient = mock(ProgrammaticAuthClient.class);
        when(authClient.listPersonalAccessTokens()).thenReturn(Collections.<PatInfo>emptyList());

        KeboolaJdbcException e = assertThrows(KeboolaJdbcException.class,
                () -> KeboolaConnection.resolveAuthProvider(config(FAKE_PAT), () -> authClient));

        assertEquals("28000", e.getSQLState());
    }

    /** Builds a config for the given credential plus optional key/value property pairs. */
    private static ConnectionConfig config(String credential, String... properties) throws Exception {
        Properties props = new Properties();
        props.setProperty("token", credential);
        for (int i = 0; i < properties.length; i += 2) {
            props.setProperty(properties[i], properties[i + 1]);
        }
        return ConnectionConfig.fromUrl("jdbc:keboola://connection.keboola.com", props);
    }

    private static PatInfo patWithProjects(PatInfo.ProjectAccess... projects) {
        return new PatInfo(null, "test pat", new PatInfo.Scope(true),
                Arrays.asList(projects), false, null);
    }

    /** Fails the test if project discovery is attempted. */
    private static Supplier<ProgrammaticAuthClient> noDiscoveryExpected() {
        return () -> {
            throw new AssertionError("Project discovery must not be called");
        };
    }
}
