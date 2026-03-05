package com.keboola.jdbc;

import java.sql.*;
import java.util.Properties;

/**
 * Manual test runner for the Keboola JDBC driver.
 * Run with: java -cp target/keboola-jdbc-driver-1.2.0.jar com.keboola.jdbc.ManualConnectionTest
 *
 * Requires env vars: KEBOOLA_TOKEN, KEBOOLA_HOST (optional, defaults to connection.keboola.com)
 */
public class ManualConnectionTest {

    public static void main(String[] args) throws Exception {
        String token = System.getenv("KEBOOLA_TOKEN");
        String host = System.getenv("KEBOOLA_HOST");
        if (host == null || host.isEmpty()) {
            host = "connection.keboola.com";
        }

        if (token == null || token.isEmpty()) {
            System.err.println("Set KEBOOLA_TOKEN environment variable");
            System.exit(1);
        }

        String workspace = System.getenv("KEBOOLA_WORKSPACE");

        String url = "jdbc:keboola://" + host;
        Properties props = new Properties();
        props.setProperty("token", token);
        if (workspace != null && !workspace.isEmpty()) {
            props.setProperty("workspace", workspace);
        }

        System.out.println("=== Keboola JDBC Driver Manual Test ===");
        System.out.println("URL: " + url);
        System.out.println();

        try (Connection conn = DriverManager.getConnection(url, props)) {
            System.out.println("Connected! Catalog: " + conn.getCatalog());
            System.out.println();

            // Test metadata
            DatabaseMetaData meta = conn.getMetaData();

            System.out.println("--- Schemas (buckets) ---");
            try (ResultSet rs = meta.getSchemas()) {
                while (rs.next()) {
                    System.out.println("  " + rs.getString("TABLE_SCHEM"));
                }
            }
            System.out.println();

            System.out.println("--- Tables ---");
            try (ResultSet rs = meta.getTables(null, null, null, null)) {
                while (rs.next()) {
                    System.out.println("  " + rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME"));
                }
            }
            System.out.println();

            // Test SQL execution
            String sql = "SELECT \"item_id\", \"name\" FROM \"in.c-main\".\"items_catalog\" LIMIT 5";
            System.out.println("--- Executing: " + sql + " ---");
            try (Statement stmt = conn.createStatement()) {
                boolean hasRs = stmt.execute(sql);
                System.out.println("execute() returned: " + hasRs);

                if (hasRs) {
                    try (ResultSet rs = stmt.getResultSet()) {
                        ResultSetMetaData rsMeta = rs.getMetaData();
                        int colCount = rsMeta.getColumnCount();
                        System.out.println("Columns: " + colCount);
                        for (int i = 1; i <= colCount; i++) {
                            System.out.print("  " + rsMeta.getColumnName(i) + " (" + rsMeta.getColumnTypeName(i) + ")");
                        }
                        System.out.println();

                        int rowNum = 0;
                        while (rs.next()) {
                            rowNum++;
                            StringBuilder sb = new StringBuilder("  Row " + rowNum + ": ");
                            for (int i = 1; i <= colCount; i++) {
                                if (i > 1) sb.append(", ");
                                sb.append(rs.getString(i));
                            }
                            System.out.println(sb);
                        }
                        System.out.println("Total rows: " + rowNum);
                    }
                } else {
                    System.out.println("Update count: " + stmt.getUpdateCount());
                }
            } catch (Exception e) {
                System.err.println("SQL execution error (non-fatal): " + e.getMessage());
            }

            System.out.println();

            // Test KEBOOLA HELP
            System.out.println("--- KEBOOLA HELP ---");
            try (Statement stmt = conn.createStatement()) {
                boolean hasRs = stmt.execute("KEBOOLA HELP");
                if (hasRs) {
                    try (ResultSet rs = stmt.getResultSet()) {
                        ResultSetMetaData rsMeta = rs.getMetaData();
                        int colCount = rsMeta.getColumnCount();
                        for (int i = 1; i <= colCount; i++) {
                            System.out.print(rsMeta.getColumnName(i) + "\t");
                        }
                        System.out.println();
                        while (rs.next()) {
                            for (int i = 1; i <= colCount; i++) {
                                System.out.print(rs.getString(i) + "\t");
                            }
                            System.out.println();
                        }
                    }
                }
            }
            System.out.println();

            // Test virtual tables
            String[] virtualTables = {"_keboola.components", "_keboola.events LIMIT 5", "_keboola.jobs LIMIT 5"};
            for (String vt : virtualTables) {
                String vtSql = "SELECT * FROM " + vt;
                System.out.println("--- " + vtSql + " ---");
                try (Statement stmt = conn.createStatement()) {
                    boolean hasRs = stmt.execute(vtSql);
                    if (hasRs) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            ResultSetMetaData rsMeta = rs.getMetaData();
                            int colCount = rsMeta.getColumnCount();
                            for (int i = 1; i <= colCount; i++) {
                                System.out.print(rsMeta.getColumnName(i) + "\t");
                            }
                            System.out.println();
                            int rowNum = 0;
                            while (rs.next()) {
                                rowNum++;
                                for (int i = 1; i <= colCount; i++) {
                                    System.out.print(rs.getString(i) + "\t");
                                }
                                System.out.println();
                            }
                            System.out.println("Total rows: " + rowNum);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Virtual table error: " + e.getMessage());
                }
                System.out.println();
            }

            System.out.println("=== Test complete ===");
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
