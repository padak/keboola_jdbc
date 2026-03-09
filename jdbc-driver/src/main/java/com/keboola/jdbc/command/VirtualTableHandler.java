package com.keboola.jdbc.command;

import com.keboola.jdbc.KeboolaConnection;
import com.keboola.jdbc.config.DriverConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles SQL queries against virtual {@code _keboola.*} tables.
 * Detects the virtual table name and optional LIMIT clause, then delegates
 * to {@link VirtualTableRegistry} for data retrieval.
 */
public class VirtualTableHandler implements KeboolaCommandHandler {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualTableHandler.class);

    /**
     * Matches any SQL containing _keboola.{table_name} reference,
     * with or without double-quote escaping (DBeaver sends "_keboola"."events").
     * Captures the table name (group 1).
     */
    private static final Pattern VIRTUAL_TABLE_PATTERN = Pattern.compile(
            "\"?_keboola\"?\\.\"?(\\w+)\"?",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Extracts LIMIT value from SQL (group 1).
     */
    private static final Pattern LIMIT_PATTERN = Pattern.compile(
            "LIMIT\\s+(\\d+)",
            Pattern.CASE_INSENSITIVE
    );

    @Override
    public boolean canHandle(String sql) {
        return VIRTUAL_TABLE_PATTERN.matcher(sql).find();
    }

    @Override
    public ResultSet execute(String sql, KeboolaConnection connection) throws SQLException {
        Matcher tableMatcher = VIRTUAL_TABLE_PATTERN.matcher(sql);
        if (!tableMatcher.find()) {
            throw new SQLException("Could not extract virtual table name from: " + sql);
        }
        String tableName = tableMatcher.group(1).toLowerCase();

        int limit = parseLimit(sql);
        LOG.debug("Virtual table query: table={}, limit={}", tableName, limit);

        return VirtualTableRegistry.query(tableName, limit, connection);
    }

    /**
     * Extracts LIMIT value from SQL, or returns the default.
     */
    static int parseLimit(String sql) {
        Matcher m = LIMIT_PATTERN.matcher(sql);
        if (m.find()) {
            try {
                return Integer.parseInt(m.group(1));
            } catch (NumberFormatException e) {
                return DriverConfig.VIRTUAL_TABLE_DEFAULT_LIMIT;
            }
        }
        return DriverConfig.VIRTUAL_TABLE_DEFAULT_LIMIT;
    }
}
