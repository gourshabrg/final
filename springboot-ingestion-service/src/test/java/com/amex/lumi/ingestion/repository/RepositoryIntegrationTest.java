package com.amex.lumi.ingestion.repository;

import com.amex.lumi.ingestion.dto.IngestionStatusResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Reads from the real warehouse. Skipped when the Docker database is not running.
 * URL order: LUMI_WAREHOUSE_JDBC_URL env var, then the project .env file, then localhost:5432.
 */
class RepositoryIntegrationTest {

    private static final JdbcClient JDBC = JdbcClient.create(
            new DriverManagerDataSource(findUrl(), "airflow", "airflow"));

    private UUID executionId;
    private String employeeId;

    @BeforeEach
    void setUp() {
        assumeTrue(databaseIsUp(), "warehouse database is not running");
        executionId = UUID.randomUUID();
        employeeId = "S" + ThreadLocalRandom.current().nextInt(100000, 999999);
    }

    @AfterEach
    void cleanUp() {
        if (executionId != null) {
            JDBC.sql("DELETE FROM employee WHERE execution_id = ?").param(executionId).update();
            JDBC.sql("DELETE FROM ingestion_error WHERE execution_id = ?").param(executionId).update();
            JDBC.sql("DELETE FROM ingestion_execution WHERE execution_id = ?").param(executionId).update();
        }
    }

    @Test
    void findsExecutionWithItsErrorCount() {
        JDBC.sql("""
                INSERT INTO ingestion_execution (execution_id, source_file, status, expected_record_count,
                    actual_loaded_record_count, started_at, completed_at, failure_reason)
                VALUES (?, 'in.csv', 'FAILED', 3, 2, now(), now(), 'Record count mismatch')
                """).param(executionId).update();
        JDBC.sql("""
                INSERT INTO ingestion_error (execution_id, source_file, record_number, error_type, error_message)
                VALUES (?, 'in.csv', 3, 'VALIDATION_ERROR', 'email is required')
                """).param(executionId).update();

        Optional<IngestionStatusResponse> status = new IngestionExecutionRepository(JDBC).findById(executionId);

        assertThat(status).isPresent();
        assertThat(status.get().status()).isEqualTo("FAILED");
        assertThat(status.get().expectedRecordCount()).isEqualTo(3L);
        assertThat(status.get().errorRecordCount()).isEqualTo(1);
        assertThat(status.get().failureReason()).isEqualTo("Record count mismatch");
    }

    @Test
    void unknownExecutionIsEmpty() {
        assertThat(new IngestionExecutionRepository(JDBC).findById(UUID.randomUUID())).isEmpty();
    }

    @Test
    void findsEmployeeWithJsonColumnsAsText() {
        JDBC.sql("""
                INSERT INTO employee (employee_id, first_name, email, phone_number_encrypted, hire_date,
                    salary_encrypted, currency, is_active, skills, address, emergency_contact,
                    ingestion_timestamp, execution_id, source_creation_time)
                VALUES (?, 'Arjun', 'arjun@techcorp.com', 'v1:p', DATE '2022-03-15', 'v1:s', 'INR', true,
                    '["Java"]'::jsonb, '{"city":"Pune"}'::jsonb, '{"phone":"v1:e"}'::jsonb, now(), ?, now())
                """).params(employeeId, executionId).update();

        Optional<EmployeeRow> row = new EmployeeRepository(JDBC).findById(employeeId);

        assertThat(row).isPresent();
        assertThat(row.get().salaryEncrypted()).isEqualTo("v1:s");
        assertThat(row.get().skillsJson()).isEqualTo("[\"Java\"]");
        assertThat(row.get().executionId()).isEqualTo(executionId);
    }

    @Test
    void unknownEmployeeIsEmpty() {
        assertThat(new EmployeeRepository(JDBC).findById("NOPE000")).isEmpty();
    }

    private static boolean databaseIsUp() {
        try {
            JDBC.sql("SELECT 1").query(Integer.class).single();
            return true;
        } catch (RuntimeException exception) {
            System.out.println("Warehouse not reachable, skipping database tests: " + exception.getMessage());
            return false;
        }
    }

    private static String findUrl() {
        String fromEnv = System.getenv("LUMI_WAREHOUSE_JDBC_URL");
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        try {
            Path envFile = Path.of("..", ".env");
            if (Files.isRegularFile(envFile)) {
                for (String line : Files.readAllLines(envFile)) {
                    String trimmed = line.strip().replace("﻿", "");
                    if (trimmed.startsWith("LUMI_WAREHOUSE_JDBC_URL=")) {
                        return trimmed.substring("LUMI_WAREHOUSE_JDBC_URL=".length());
                    }
                }
            }
        } catch (Exception ignored) {
            // Fall back to the default URL.
        }
        return "jdbc:postgresql://localhost:5432/warehouse";
    }
}
