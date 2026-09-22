package com.amex.lumi.beam.model;

import java.io.Serializable;
import java.time.Instant;

/**
 * One row of the ingestion_execution table. Create it with the static methods below.
 */
public record IngestionExecution(
        String executionId,
        String sourceFile,
        ExecutionStatus status,
        Long expectedRecordCount,
        Long actualLoadedRecordCount,
        Instant startedAt,
        Instant completedAt,
        String failureReason) implements Serializable {

    public static IngestionExecution started(
            String executionId, String sourceFile, long expectedRecordCount, Instant startedAt) {
        return new IngestionExecution(
                executionId, sourceFile, ExecutionStatus.STARTED,
                expectedRecordCount, null, startedAt, null, null);
    }

    public static IngestionExecution running(
            String executionId, String sourceFile, long expectedRecordCount, Instant startedAt) {
        return new IngestionExecution(
                executionId, sourceFile, ExecutionStatus.RUNNING,
                expectedRecordCount, null, startedAt, null, null);
    }

    public static IngestionExecution succeeded(
            String executionId, String sourceFile, long expectedRecordCount,
            long actualLoadedRecordCount, Instant startedAt, Instant completedAt) {
        return new IngestionExecution(
                executionId, sourceFile, ExecutionStatus.SUCCESS,
                expectedRecordCount, actualLoadedRecordCount, startedAt, completedAt, null);
    }

    /** expectedRecordCount is null when the control file itself could not be read. */
    public static IngestionExecution failed(
            String executionId, String sourceFile, Long expectedRecordCount,
            Long actualLoadedRecordCount, Instant startedAt, Instant completedAt, String failureReason) {
        return new IngestionExecution(
                executionId, sourceFile, ExecutionStatus.FAILED,
                expectedRecordCount, actualLoadedRecordCount, startedAt, completedAt, failureReason);
    }
}
