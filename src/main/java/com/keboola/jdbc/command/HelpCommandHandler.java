package com.keboola.jdbc.command;

import com.keboola.jdbc.ArrayResultSet;
import com.keboola.jdbc.KeboolaConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Handles the {@code KEBOOLA HELP} command and returns a table listing
 * all available Keboola commands and virtual tables.
 */
public class HelpCommandHandler implements KeboolaCommandHandler {

    private static final Pattern HELP_PATTERN = Pattern.compile(
            "^\\s*KEBOOLA\\s+HELP\\s*;?\\s*$",
            Pattern.CASE_INSENSITIVE
    );

    @Override
    public boolean canHandle(String sql) {
        return HELP_PATTERN.matcher(sql).matches();
    }

    @Override
    public ResultSet execute(String sql, KeboolaConnection connection) throws SQLException {
        List<String> columns = Arrays.asList("command", "syntax", "description");
        List<List<Object>> rows = new ArrayList<>();

        rows.add(Arrays.asList("HELP", "KEBOOLA HELP", "Show this help"));
        rows.add(Arrays.asList("Backend", "KEBOOLA USE BACKEND duckdb", "Switch to local DuckDB backend"));
        rows.add(Arrays.asList("Backend", "KEBOOLA USE BACKEND queryservice", "Switch to Keboola Query Service backend"));
        rows.add(Arrays.asList("Pull", "KEBOOLA PULL TABLE stage.bucket.table [INTO name]", "Pull table from Keboola into DuckDB"));
        rows.add(Arrays.asList("Pull", "KEBOOLA PULL QUERY <sql> INTO name", "Run query on Keboola, save results to DuckDB"));
        rows.add(Arrays.asList("Push", "KEBOOLA PUSH TABLE local_table [INTO stage.bucket.table]", "Push DuckDB table to Keboola/Snowflake"));
        rows.add(Arrays.asList("Session", "KEBOOLA SESSION LOG", "Show SQL execution log (dialect analysis)"));
        rows.add(Arrays.asList("Virtual table", "SELECT * FROM _keboola.components", "List configured components"));
        rows.add(Arrays.asList("Virtual table", "SELECT * FROM _keboola.events [LIMIT n]", "List recent storage events"));
        rows.add(Arrays.asList("Virtual table", "SELECT * FROM _keboola.jobs [LIMIT n]", "List recent jobs"));
        rows.add(Arrays.asList("Virtual table", "SELECT * FROM _keboola.tables", "List all tables with metadata"));
        rows.add(Arrays.asList("Virtual table", "SELECT * FROM _keboola.buckets", "List all buckets"));

        return new ArrayResultSet(columns, rows);
    }
}
