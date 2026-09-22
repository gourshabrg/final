package com.amex.lumi.beam.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ModelTest {

    private static final Instant START = Instant.parse("2026-01-01T10:00:00Z");
    private static final Instant END = Instant.parse("2026-01-01T10:05:00Z");

    @Test
    void fileTypeIgnoresLetterCase() {
        assertEquals(FileType.CSV, FileType.from("csv"));
        assertEquals(FileType.JSON, FileType.from(" JSON "));
    }

    @Test
    void fileTypeRejectsUnsupportedValue() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> FileType.from("XML"));
        assertTrue(error.getMessage().contains("Supported values: CSV, JSON"));
    }

    @Test
    void fileTypeIsRequired() {
        assertThrows(IllegalArgumentException.class, () -> FileType.from(" "));
    }

    @Test
    void countCheckMatchesWhenEqual() {
        assertTrue(new RecordCountCheckResult(20, 20).matched());
    }

    @Test
    void countCheckExplainsMismatch() {
        RecordCountCheckResult result = new RecordCountCheckResult(20, 16);
        assertFalse(result.matched());
        assertEquals("Record count mismatch: control file expected 20 record(s) but 16 record(s) were loaded",
                result.failureMessage());
    }

    @Test
    void startedAndRunningHaveNoEndTime() {
        IngestionExecution started = IngestionExecution.started("id", "in.csv", 5, START);
        IngestionExecution running = IngestionExecution.running("id", "in.csv", 5, START);

        assertEquals(ExecutionStatus.STARTED, started.status());
        assertEquals(ExecutionStatus.RUNNING, running.status());
        assertNull(running.completedAt());
        assertNull(running.actualLoadedRecordCount());
    }

    @Test
    void succeededHasCountsAndNoReason() {
        IngestionExecution success = IngestionExecution.succeeded("id", "in.csv", 5, 5, START, END);

        assertEquals(ExecutionStatus.SUCCESS, success.status());
        assertEquals(5L, success.actualLoadedRecordCount());
        assertNull(success.failureReason());
    }

    @Test
    void failedKeepsReasonAndAllowsUnknownCounts() {
        IngestionExecution failed = IngestionExecution.failed("id", "in.csv", null, null, START, END, "bad file");

        assertEquals(ExecutionStatus.FAILED, failed.status());
        assertEquals("bad file", failed.failureReason());
        assertNull(failed.expectedRecordCount());
    }

    @Test
    void employeeToStringHidesSensitiveFields() {
        EmployeeRecord employee = new EmployeeRecord();
        employee.setEmployeeId("EMP0001");
        employee.setSalary(950000L);
        employee.setPhoneNumber("9876543210");

        assertEquals("EmployeeRecord{employeeId='EMP0001'}", employee.toString());
    }
}
