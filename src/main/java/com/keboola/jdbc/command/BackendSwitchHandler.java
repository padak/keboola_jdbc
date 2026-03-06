package com.keboola.jdbc.command;

import com.keboola.jdbc.ArrayResultSet;
import com.keboola.jdbc.KeboolaConnection;
import com.keboola.jdbc.config.DriverConfig;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles the {@code KEBOOLA USE BACKEND <type>} command to switch between
 * Query Service and DuckDB backends at runtime.
 */
public class BackendSwitchHandler implements KeboolaCommandHandler {

    private static final Pattern PATTERN = Pattern.compile(
            "^\\s*KEBOOLA\\s+USE\\s+BACKEND\\s+(\\w+)\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );

    @Override
    public boolean canHandle(String sql) {
        return sql != null && PATTERN.matcher(sql.trim()).matches();
    }

    @Override
    public ResultSet execute(String sql, KeboolaConnection connection) throws SQLException {
        Matcher matcher = PATTERN.matcher(sql.trim());
        if (!matcher.matches()) {
            throw new SQLException("Failed to parse backend switch command: " + sql);
        }

        String targetBackend = matcher.group(1).toLowerCase();
        connection.switchBackend(targetBackend);

        List<String> columns = Arrays.asList("STATUS", "BACKEND", "MESSAGE");
        List<List<Object>> rows = Collections.singletonList(
                Arrays.asList("OK", targetBackend, "Backend switched to: " + targetBackend)
        );
        return new ArrayResultSet(columns, rows);
    }
}
