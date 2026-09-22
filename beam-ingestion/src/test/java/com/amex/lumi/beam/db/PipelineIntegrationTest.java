package com.amex.lumi.beam.db;

import com.amex.lumi.beam.TestDatabase;
import com.amex.lumi.beam.TestEmployees;
import com.amex.lumi.beam.execution.RecordCountCheck;
import com.amex.lumi.beam.options.IngestionPipelineOptions;
import com.amex.lumi.beam.pipeline.IngestionGraphRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Runs the whole Beam graph against the real warehouse. Skipped when the database is not running. */
class PipelineIntegrationTest {

    @TempDir
    Path tempDir;

    private UUID executionId;

    @BeforeEach
    void setUp() {
        assumeTrue(TestDatabase.isAvailable(), "warehouse database is not running");
        executionId = UUID.randomUUID();
    }

    @AfterEach
    void cleanUp() throws Exception {
        if (executionId != null && TestDatabase.isAvailable()) {
            TestDatabase.cleanUp(executionId);
        }
    }

    @Test
    void matchingCountEndsInSuccess() throws Exception {
        Path csv = writeCsv(randomRow(), randomRow());

        IngestionGraphRunner.run(options(csv), run(csv, 2));

        assertEquals("SUCCESS", status());
        assertEquals(2, TestDatabase.count("SELECT COUNT(*) FROM employee WHERE execution_id = ?", executionId));
    }

    @Test
    void mismatchFailsTheRunAndStoresTheReason() throws Exception {
        Path csv = writeCsv(randomRow(), randomRow().replace(",true,", ",maybe,"));

        assertThrows(RuntimeException.class, () -> IngestionGraphRunner.run(options(csv), run(csv, 2)));

        assertEquals("FAILED", status());
        assertTrue(TestDatabase.queryString("SELECT failure_reason FROM ingestion_execution WHERE execution_id = ?",
                executionId).contains("expected 2 record(s) but 1 record(s) were loaded"));
        assertEquals(1, TestDatabase.count("SELECT COUNT(*) FROM ingestion_error WHERE execution_id = ?",
                executionId));
    }

    private Path writeCsv(String... rows) throws Exception {
        Path csv = tempDir.resolve("employees.csv");
        Files.writeString(csv, TestEmployees.CSV_HEADER + "\n" + String.join("\n", rows) + "\n");
        return csv;
    }

    private static String randomRow() {
        return TestEmployees.CSV_ROW.replace("EMP0001", "T" + ThreadLocalRandom.current().nextInt(100000, 999999));
    }

    private IngestionPipelineOptions options(Path csv) {
        String[] args = {
                "--inputFile=" + csv, "--fileType=CSV", "--executionId=" + executionId,
                "--controlFile=unused", "--errorOutput=" + tempDir.resolve("errors"),
                "--encryptionKey=0123456789abcdef0123456789abcdef",
                "--jdbcUrl=" + TestDatabase.CONFIG.jdbcUrl(),
                "--jdbcUsername=" + TestDatabase.CONFIG.username(),
                "--jdbcPassword=" + TestDatabase.CONFIG.password()};
        return org.apache.beam.sdk.options.PipelineOptionsFactory.fromArgs(args).as(IngestionPipelineOptions.class);
    }

    private RecordCountCheck.RunContext run(Path csv, long expected) {
        return new RecordCountCheck.RunContext(executionId.toString(), csv.toString(), expected, Instant.now());
    }

    private String status() throws Exception {
        return TestDatabase.queryString("SELECT status FROM ingestion_execution WHERE execution_id = ?", executionId);
    }
}
