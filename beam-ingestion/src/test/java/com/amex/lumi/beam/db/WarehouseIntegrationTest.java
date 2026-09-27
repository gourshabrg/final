package com.amex.lumi.beam.db;

import com.amex.lumi.beam.TestDatabase;
import com.amex.lumi.beam.TestEmployees;
import com.amex.lumi.beam.execution.ExecutionStatusRepository;
import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.model.EncryptedEmployee;
import com.amex.lumi.beam.model.EnrichedEmployee;
import com.amex.lumi.beam.model.IngestionExecution;
import com.amex.lumi.beam.model.ParsedEmployee;
import com.amex.lumi.beam.model.RecordFailure;
import com.amex.lumi.beam.model.RecordFailure.FailureType;
import com.amex.lumi.beam.write.LoadEmployees;
import com.amex.lumi.beam.write.WriteErrorReport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptors;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Writes to the real warehouse tables. Skipped when the Docker database is not running. */
class WarehouseIntegrationTest {

    @TempDir
    Path tempDir;

    private UUID executionId;
    private ExecutionStatusRepository repository;

    @BeforeEach
    void setUp() {
        assumeTrue(TestDatabase.isAvailable(), "warehouse database is not running");
        executionId = UUID.randomUUID();
        repository = new ExecutionStatusRepository(TestDatabase.CONFIG);
    }

    @AfterEach
    void cleanUp() throws Exception {
        if (executionId != null && TestDatabase.isAvailable()) {
            TestDatabase.cleanUp(executionId);
        }
    }

    @Test
    void savesStatusChangesForOneRun() throws Exception {
        Instant start = Instant.now();
        repository.save(IngestionExecution.started(executionId.toString(), "in.csv", 2, start));
        repository.save(IngestionExecution.succeeded(executionId.toString(), "in.csv", 2, 2, start, Instant.now()));

        assertEquals("SUCCESS", status());
        assertEquals(1, TestDatabase.count("SELECT COUNT(*) FROM ingestion_execution WHERE execution_id = ?",
                executionId));
    }

    @Test
    void crashMarksRunningRunAsFailed() throws Exception {
        repository.save(IngestionExecution.running(executionId.toString(), "in.csv", 2, Instant.now()));

        repository.markFailedIfUnfinished(executionId.toString(), "disk full");

        assertEquals("FAILED", status());
        assertEquals("disk full", TestDatabase.queryString(
                "SELECT failure_reason FROM ingestion_execution WHERE execution_id = ?", executionId));
    }

    @Test
    void crashDoesNotOverwriteFinishedRun() throws Exception {
        Instant start = Instant.now();
        repository.save(IngestionExecution.succeeded(executionId.toString(), "in.csv", 2, 2, start, Instant.now()));

        repository.markFailedIfUnfinished(executionId.toString(), "late error");

        assertEquals("SUCCESS", status());
    }

    @Test
    void loadsGoodRowsAndRejectsRowsTheDatabaseRefuses() throws Exception {
        EmployeeRecord good = TestEmployees.valid();
        good = good.toBuilder().employeeId(randomEmployeeId()).build();
        EmployeeRecord tooLong = TestEmployees.valid();
        tooLong = tooLong.toBuilder().employeeId(randomEmployeeId()).build();
        tooLong = tooLong.toBuilder().firstName("ThisNameIsFarTooLongForTheColumn").build();

        Pipeline pipeline = Pipeline.create();
        PCollectionTuple result = pipeline
                .apply(Create.of(row(1, good), row(2, tooLong)))
                .apply(new LoadEmployees(TestDatabase.CONFIG));
        PAssert.that(result.get(LoadEmployees.FAILED)
                        .apply("FailedTypes", MapElements.into(TypeDescriptors.strings())
                                .via(failure -> failure.recordNumber() + ":" + failure.type())))
                .containsInAnyOrder("2:" + FailureType.LOAD_ERROR);
        pipeline.run().waitUntilFinish();

        assertEquals(1, TestDatabase.count("SELECT COUNT(*) FROM employee WHERE execution_id = ?", executionId));
        assertTrue(TestDatabase.queryString(
                "SELECT salary_encrypted FROM employee WHERE employee_id = ?", good.getEmployeeId()).startsWith("v1:"));
    }

    @Test
    void errorReportGoesToFileAndTableWithRedaction() throws Exception {
        String prefix = tempDir.resolve("errors").toString();
        RecordFailure failure = new RecordFailure(3, "in.csv", executionId.toString(),
                FailureType.VALIDATION_ERROR, "email is required", TestEmployees.valid());

        Pipeline pipeline = Pipeline.create();
        pipeline.apply(Create.of(failure)).apply(new WriteErrorReport(prefix, TestDatabase.CONFIG));
        pipeline.run().waitUntilFinish();

        assertTrue(Files.readString(Path.of(prefix + ".txt")).contains("error_message=email is required"));
        assertEquals("[REDACTED]", TestDatabase.queryString(
                "SELECT raw_record->>'salary' FROM ingestion_error WHERE execution_id = ?", executionId));
    }

    @Test
    void secondEmployeeWithTheSameEmailIsRejected() throws Exception {
        EmployeeRecord first = TestEmployees.valid();
        first = first.toBuilder().employeeId(randomEmployeeId()).build();
        EmployeeRecord second = TestEmployees.valid();
        second = second.toBuilder().employeeId(randomEmployeeId()).build();

        load(row(1, first));
        Pipeline pipeline = Pipeline.create();
        PCollectionTuple result = pipeline.apply(Create.of(row(2, second)))
                .apply(new LoadEmployees(TestDatabase.CONFIG));
        PAssert.that(result.get(LoadEmployees.FAILED)
                        .apply("FailedTypes", MapElements.into(TypeDescriptors.strings())
                                .via(failure -> failure.type().name())))
                .containsInAnyOrder(FailureType.LOAD_ERROR.name());
        pipeline.run().waitUntilFinish();

        assertEquals(1, TestDatabase.count("SELECT COUNT(*) FROM employee WHERE execution_id = ?", executionId));
    }

    @Test
    void olderFileDoesNotOverwriteNewerData() throws Exception {
        EmployeeRecord newer = TestEmployees.valid();
        newer = newer.toBuilder().employeeId(randomEmployeeId()).build();
        newer = newer.toBuilder().firstName("Newer").build();
        EmployeeRecord older = TestEmployees.valid();
        older = older.toBuilder().employeeId(newer.getEmployeeId()).build();
        older = older.toBuilder().firstName("Older").build();
        Instant today = Instant.now();

        load(row(1, newer, today));
        Pipeline pipeline = Pipeline.create();
        PCollectionTuple result = pipeline.apply(Create.of(row(1, older, today.minusSeconds(86_400))))
                .apply(new LoadEmployees(TestDatabase.CONFIG));
        PAssert.that(result.get(LoadEmployees.FAILED)
                        .apply("FailedTypes", MapElements.into(TypeDescriptors.strings())
                                .via(failure -> failure.type().name())))
                .containsInAnyOrder(FailureType.STALE_ERROR.name());
        pipeline.run().waitUntilFinish();

        assertEquals("Newer", TestDatabase.queryString(
                "SELECT first_name FROM employee WHERE employee_id = ?", newer.getEmployeeId()));
    }

    @Test
    void errorFromSplitFileStoresOriginalFileAndPartFile() throws Exception {
        RecordFailure failure = new RecordFailure(87, "/data/employees_large.csv", "/data/split/part-00001.csv",
                executionId.toString(), FailureType.PARSE_ERROR, "salary must be a whole number", null);

        Pipeline pipeline = Pipeline.create();
        pipeline.apply(Create.of(failure))
                .apply(new WriteErrorReport(tempDir.resolve("errors").toString(), TestDatabase.CONFIG));
        pipeline.run().waitUntilFinish();

        assertEquals("87|/data/employees_large.csv|/data/split/part-00001.csv", TestDatabase.queryString(
                "SELECT record_number || '|' || source_file || '|' || split_file FROM ingestion_error"
                        + " WHERE execution_id = ?", executionId));
    }

    private String status() throws Exception {
        return TestDatabase.queryString("SELECT status FROM ingestion_execution WHERE execution_id = ?", executionId);
    }

    private EncryptedEmployee row(long recordNumber, EmployeeRecord employee) {
        return row(recordNumber, employee, null);
    }

    private EncryptedEmployee row(long recordNumber, EmployeeRecord employee, Instant sourceModifiedAt) {
        ParsedEmployee parsed = new ParsedEmployee(recordNumber, "in.csv", Instant.now(), employee);
        return new EncryptedEmployee(
                new EnrichedEmployee(parsed, executionId.toString(), Instant.now(), sourceModifiedAt),
                "v1:phone", "v1:salary", "v1:ec-phone");
    }

    private static void load(EncryptedEmployee row) {
        Pipeline pipeline = Pipeline.create();
        pipeline.apply(Create.of(row)).apply(new LoadEmployees(TestDatabase.CONFIG));
        pipeline.run().waitUntilFinish();
    }

    private static String randomEmployeeId() {
        return "T" + ThreadLocalRandom.current().nextInt(100000, 999999);
    }
}
