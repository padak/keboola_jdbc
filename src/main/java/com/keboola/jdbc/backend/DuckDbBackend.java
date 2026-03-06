package com.keboola.jdbc.backend;

import com.keboola.jdbc.config.DriverConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

/**
 * Backend implementation using an embedded DuckDB database (local, synchronous JDBC).
 * Supports both file-based persistence and in-memory ephemeral mode.
 *
 * <p>DuckDB's JDBC JAR is embedded as a resource ({@code /duckdb_jdbc.jar}) inside the uber-jar
 * rather than being merged into it. This is necessary because DuckDB's native library loader
 * ({@code DuckDBNative.<clinit>}) opens its own JAR via {@code java.util.zip.ZipFile} to extract
 * the platform-specific native library. When DuckDB's classes are merged into the uber-jar by
 * maven-shade-plugin, the ZIP local file headers can become unreadable in certain classloader
 * contexts (DBeaver, DataGrip), causing {@code ZipException: invalid LOC header}.
 *
 * <p>By keeping DuckDB as a separate nested JAR and loading it via a child {@link URLClassLoader},
 * DuckDB's native loader reads from a clean, intact JAR file and works reliably everywhere.
 */
public class DuckDbBackend implements QueryBackend {

    private static final Logger LOG = LoggerFactory.getLogger(DuckDbBackend.class);

    /** Resource path to the embedded DuckDB JAR inside the uber-jar. */
    private static final String DUCKDB_JAR_RESOURCE = "/duckdb_jdbc.jar";

    /** Cached path to the extracted DuckDB JAR temp file. Shared across all instances. */
    private static volatile Path extractedJarPath;

    /** Cached classloader for DuckDB classes. Shared across all instances. */
    private static volatile URLClassLoader duckDbClassLoader;

    private final Connection duckConnection;
    private volatile Statement currentStatement;

    /**
     * Creates a new DuckDB backend.
     *
     * @param duckdbPath file path for persistent storage, or ":memory:" for ephemeral mode
     * @throws SQLException if the DuckDB connection cannot be established
     */
    public DuckDbBackend(String duckdbPath) throws SQLException {
        String jdbcUrl = "jdbc:duckdb:" + duckdbPath;
        LOG.info("Initializing DuckDB backend: {}", jdbcUrl);
        try {
            Driver driver = loadDuckDbDriver();
            this.duckConnection = driver.connect(jdbcUrl, new Properties());
            if (this.duckConnection == null) {
                throw new SQLException("DuckDB driver returned null connection for URL: " + jdbcUrl);
            }
        } catch (SQLException e) {
            throw new SQLException("Failed to open DuckDB database at '" + duckdbPath
                    + "': " + e.getMessage(), e);
        } catch (Exception e) {
            throw new SQLException("Failed to initialize DuckDB backend at '" + duckdbPath
                    + "': " + e.getClass().getName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Loads the DuckDB JDBC driver from the embedded JAR resource.
     *
     * <p>On first call, extracts {@code /duckdb_jdbc.jar} from the uber-jar to a temp file,
     * creates a {@link URLClassLoader} for it, and instantiates {@code org.duckdb.DuckDBDriver}.
     * Subsequent calls reuse the cached classloader and driver.
     *
     * @return a DuckDB JDBC Driver instance
     * @throws Exception if extraction or class loading fails
     */
    private static synchronized Driver loadDuckDbDriver() throws Exception {
        if (duckDbClassLoader == null) {
            extractedJarPath = extractDuckDbJar();
            URL jarUrl = extractedJarPath.toUri().toURL();
            duckDbClassLoader = new URLClassLoader(
                    new URL[]{jarUrl},
                    DuckDbBackend.class.getClassLoader()
            );
            LOG.info("DuckDB classloader created from: {}", extractedJarPath);
        }

        Class<?> driverClass = duckDbClassLoader.loadClass("org.duckdb.DuckDBDriver");
        return (Driver) driverClass.getDeclaredConstructor().newInstance();
    }

    /**
     * Extracts the embedded DuckDB JAR resource to a temporary file.
     * The temp file is marked for deletion on JVM exit.
     *
     * @return path to the extracted JAR file
     * @throws SQLException if the resource is not found or extraction fails
     */
    private static Path extractDuckDbJar() throws SQLException {
        LOG.info("Extracting embedded DuckDB JAR from resource: {}", DUCKDB_JAR_RESOURCE);

        try (InputStream is = DuckDbBackend.class.getResourceAsStream(DUCKDB_JAR_RESOURCE)) {
            if (is == null) {
                throw new SQLException(
                        "DuckDB JAR not found as resource: " + DUCKDB_JAR_RESOURCE
                        + ". Ensure the uber-jar was built with 'mvn package'.");
            }

            Path tempJar = Files.createTempFile("keboola_duckdb_jdbc_", ".jar");
            tempJar.toFile().deleteOnExit();

            Files.copy(is, tempJar, StandardCopyOption.REPLACE_EXISTING);
            long size = Files.size(tempJar);
            LOG.info("DuckDB JAR extracted: {} ({} bytes)", tempJar, size);

            return tempJar;

        } catch (IOException e) {
            throw new SQLException("Failed to extract DuckDB JAR: " + e.getMessage(), e);
        }
    }

    @Override
    public ExecutionResult execute(List<String> statements) throws SQLException {
        if (statements.isEmpty()) {
            return ExecutionResult.forUpdateCount(0);
        }

        ExecutionResult lastResult = null;

        for (String sql : statements) {
            // Close previous result set if it was a query and we have more statements
            if (lastResult != null && lastResult.hasResultSet()) {
                lastResult.getResultSet().close();
            }

            Statement stmt = duckConnection.createStatement();
            this.currentStatement = stmt;

            boolean isResultSet = stmt.execute(sql);

            if (isResultSet) {
                lastResult = ExecutionResult.forResultSet(stmt.getResultSet());
            } else {
                lastResult = ExecutionResult.forUpdateCount(stmt.getUpdateCount());
                stmt.close();
            }
        }

        return lastResult;
    }

    @Override
    public void cancel() throws SQLException {
        Statement stmt = currentStatement;
        if (stmt != null) {
            stmt.cancel();
        }
    }

    @Override
    public String getCurrentCatalog() throws SQLException {
        return duckConnection.getCatalog();
    }

    @Override
    public String getCurrentSchema() throws SQLException {
        return duckConnection.getSchema();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        duckConnection.setCatalog(catalog);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        duckConnection.setSchema(schema);
    }

    @Override
    public DatabaseMetaData getNativeMetaData() throws SQLException {
        return duckConnection.getMetaData();
    }

    @Override
    public void close() throws SQLException {
        if (!duckConnection.isClosed()) {
            duckConnection.close();
            LOG.info("DuckDB backend closed");
        }
    }

    @Override
    public String getBackendType() {
        return DriverConfig.BACKEND_DUCKDB;
    }

    /**
     * Returns the underlying DuckDB JDBC connection (for advanced use cases).
     */
    public Connection getNativeConnection() {
        return duckConnection;
    }
}
