package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.model.IngestionExecution;
import com.amex.lumi.beam.model.IngestionExecutionStatus;

import java.time.Instant;

/**
 * Creates immutable lifecycle snapshots for the ingestion execution table.
 */
public class IngestionExecutionFactory {

    public IngestionExecution createStarted(
            String executionId,
            String sourceFile,
            long expectedRecordCount,
            Instant startedAt) {

        return new IngestionExecution(
                executionId,
                sourceFile,
                IngestionExecutionStatus.STARTED,
                expectedRecordCount,
                null,
                startedAt,
                null,
                null
        );
    }

    public IngestionExecution createRunning(
            String executionId,
            String sourceFile,
            long expectedRecordCount,
            Instant startedAt) {

        return new IngestionExecution(
                executionId,
                sourceFile,
                IngestionExecutionStatus.RUNNING,
                expectedRecordCount,
                null,
                startedAt,
                null,
                null
        );
    }

    public IngestionExecution createSuccess(
            String executionId,
            String sourceFile,
            long expectedRecordCount,
            long actualLoadedRecordCount,
            Instant startedAt,
            Instant completedAt) {

        return new IngestionExecution(
                executionId,
                sourceFile,
                IngestionExecutionStatus.SUCCESS,
                expectedRecordCount,
                actualLoadedRecordCount,
                startedAt,
                completedAt,
                null
        );
    }

    public IngestionExecution createFailed(
            String executionId,
            String sourceFile,
            long expectedRecordCount,
            Long actualLoadedRecordCount,
            Instant startedAt,
            Instant completedAt,
            String failureReason) {

        return new IngestionExecution(
                executionId,
                sourceFile,
                IngestionExecutionStatus.FAILED,
                expectedRecordCount,
                actualLoadedRecordCount,
                startedAt,
                completedAt,
                failureReason
        );
    }
}
