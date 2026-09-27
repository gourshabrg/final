package com.amex.lumi.beam;

import com.amex.lumi.beam.write.DatabaseConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Docker warehouse for integration tests; tests are skipped when it is down.
 */
public final class TestDatabase {

    private static final String DEFAULT_URL = "jdbc:postgresql://localhost:5432/warehouse";
    private static final String USER = setting("POSTGRES_USER", "airflow");
    private static final String PASSWORD = setting("POSTGRES_PASSWORD", "airflow");

    public static final DatabaseConfig CONFIG =
            new DatabaseConfig(setting("LUMI_WAREHOUSE_JDBC_URL", DEFAULT_URL), USER, PASSWORD);

    private static Boolean available;

    private TestDatabase() {
    }

    public static synchronized boolean isAvailable() {
        if (available == null) {
            DriverManager.setLoginTimeout(3);
            try (Connection ignored = CONFIG.openConnection()) {
                available = true;
            } catch (SQLException exception) {
                System.out.println("Warehouse not reachable, skipping database tests: " + exception.getMessage());
                available = false;
            }
        }
        return available;
    }

    public static long count(String sql, UUID executionId) throws SQLException {
        try (Connection connection = CONFIG.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, executionId);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getLong(1);
            }
        }
    }

    public static String queryString(String sql, Object parameter) throws SQLException {
        try (Connection connection = CONFIG.openConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, parameter);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? result.getString(1) : null;
            }
        }
    }

    /** Removes everything a test wrote for one execution. */
    public static void cleanUp(UUID executionId) throws SQLException {
        try (Connection connection = CONFIG.openConnection()) {
            for (String table : new String[]{"employee", "ingestion_error", "ingestion_execution"}) {
                try (PreparedStatement statement = connection.prepareStatement(
                        "DELETE FROM " + table + " WHERE execution_id = ?")) {
                    statement.setObject(1, executionId);
                    statement.executeUpdate();
                }
            }
            connection.commit();
        }
    }

    // Environment variable first, then the project .env file, then the default.
    private static String setting(String name, String defaultValue) {
        String fromEnv = System.getenv(name);
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        Path envFile = Path.of("..", ".env");
        if (Files.isRegularFile(envFile)) {
            try {
                for (String line : Files.readAllLines(envFile)) {
                    String trimmed = line.strip().replace("﻿", "");
                    if (trimmed.startsWith(name + "=")) {
                        return trimmed.substring(name.length() + 1);
                    }
                }
            } catch (IOException ignored) {
                // Fall back to the default.
            }
        }
        return defaultValue;
    }
}
