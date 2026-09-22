
package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.model.IngestionExecution;
import com.amex.lumi.beam.model.IngestionExecutionStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class IngestionExecutionFactoryTest {

    private final IngestionExecutionFactory factory =
            new IngestionExecutionFactory();

    private final String executionId =
            "760e8400-e29b-41d4-a716-446655440000";

    private final String sourceFile =
            "employees.json";

    private final Instant startedAt =
            Instant.parse("2026-09-15T12:00:00Z");

    private final Instant completedAt =
            Instant.parse("2026-09-15T12:01:00Z");

    @Test
    void shouldCreateStartedExecution() {

        IngestionExecution execution =
                factory.createStarted(
                        executionId,
                        sourceFile,
                        10L,
                        startedAt
                );

        assertEquals(
                executionId,
                execution.getExecutionId()
        );

        assertEquals(
                sourceFile,
                execution.getSourceFile()
        );

        assertEquals(
                IngestionExecutionStatus.STARTED,
                execution.getStatus()
        );

        assertEquals(
                10L,
                execution.getExpectedRecordCount()
        );

        assertNull(
                execution.getActualLoadedRecordCount()
        );

        assertEquals(
                startedAt,
                execution.getStartedAt()
        );

        assertNull(
                execution.getCompletedAt()
        );

        assertNull(
                execution.getFailureReason()
        );
    }

    @Test
    void shouldCreateRunningExecution() {

        IngestionExecution execution =
                factory.createRunning(
                        executionId,
                        sourceFile,
                        10L,
                        startedAt
                );

        assertEquals(
                IngestionExecutionStatus.RUNNING,
                execution.getStatus()
        );

        assertEquals(
                10L,
                execution.getExpectedRecordCount()
        );

        assertNull(
                execution.getActualLoadedRecordCount()
        );

        assertNull(
                execution.getCompletedAt()
        );

        assertNull(
                execution.getFailureReason()
        );
    }

    @Test
    void shouldCreateSuccessfulExecution() {

        IngestionExecution execution =
                factory.createSuccess(
                        executionId,
                        sourceFile,
                        10L,
                        10L,
                        startedAt,
                        completedAt
                );

        assertEquals(
                IngestionExecutionStatus.SUCCESS,
                execution.getStatus()
        );

        assertEquals(
                10L,
                execution.getExpectedRecordCount()
        );

        assertEquals(
                10L,
                execution.getActualLoadedRecordCount()
        );

        assertEquals(
                startedAt,
                execution.getStartedAt()
        );

        assertEquals(
                completedAt,
                execution.getCompletedAt()
        );

        assertNull(
                execution.getFailureReason()
        );
    }

    @Test
    void shouldCreateFailedExecution() {

        String failureReason =
                "Record count mismatch: expected=10, actual=8";

        IngestionExecution execution =
                factory.createFailed(
                        executionId,
                        sourceFile,
                        10L,
                        8L,
                        startedAt,
                        completedAt,
                        failureReason
                );

        assertEquals(
                IngestionExecutionStatus.FAILED,
                execution.getStatus()
        );

        assertEquals(
                10L,
                execution.getExpectedRecordCount()
        );

        assertEquals(
                8L,
                execution.getActualLoadedRecordCount()
        );

        assertEquals(
                completedAt,
                execution.getCompletedAt()
        );

        assertEquals(
                failureReason,
                execution.getFailureReason()
        );
    }
}
