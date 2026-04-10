package com.keboola.datagrip;

import com.intellij.database.Dbms;
import com.intellij.icons.AllIcons;

/**
 * Registers Keboola as a known DBMS type in DataGrip.
 *
 * <p>The detector pattern matches the value returned by
 * {@code DatabaseMetaData.getDatabaseProductName()} from the Keboola JDBC driver.
 *
 * <p>By not registering a custom introspector, DataGrip falls back to JDBC metadata
 * introspection (getCatalogs, getSchemas, getTables, getColumns), which is exactly
 * what the Keboola JDBC driver implements with proper filtering and virtual tables.
 */
public final class KeboolaDbms {

    /**
     * The Keboola DBMS instance, detected when getDatabaseProductName() contains "Keboola".
     */
    public static final Dbms KEBOOLA = Dbms.create(
            "KEBOOLA",
            "Keboola",
            () -> AllIcons.Providers.Snowflake,
            "(?i).*Keboola.*"
    );

    private KeboolaDbms() {
        // Utility class
    }
}
