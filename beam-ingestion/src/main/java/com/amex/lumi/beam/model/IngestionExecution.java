package com.amex.lumi.beam.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Immutable lifecycle snapshot for one ingestion execution.
 */
public class IngestionExecution implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String executionId;
    private final String sourceFile;
    private final IngestionExecutionStatus status; 
    private final Long expectedRecordCount;
    private final Long actualLoadedRecordCount;
    private final Instant startedAt;
    private final Instant completedAt;
    private final String failureReason;

    public IngestionExecution(
            String executionId,
            String sourceFile,
            IngestionExecutionStatus status,
            Long expectedRecordCount,
            Long actualLoadedRecordCount,
            Instant startedAt,
            Instant completedAt,
            String failureReason) {

        this.executionId = executionId;
        this.sourceFile = sourceFile;
        this.status = status;
        this.expectedRecordCount = expectedRecordCount;
        this.actualLoadedRecordCount = actualLoadedRecordCount;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.failureReason = failureReason;
    }

    public String getExecutionId() {
        return executionId;
    }

    public String getSourceFile() {
        return sourceFile;
    }

   	public IngestionExecutionStatus getStatus() {
    return status;
}


    public Long getExpectedRecordCount() {
        return expectedRecordCount;
    }

    public Long getActualLoadedRecordCount() {
        return actualLoadedRecordCount;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public String getFailureReason() {
        return failureReason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof IngestionExecution that)) {
            return false;
        }

        return Objects.equals(executionId, that.executionId)
                && Objects.equals(sourceFile, that.sourceFile)
                && Objects.equals(status, that.status)
                && Objects.equals(
                        expectedRecordCount,
                        that.expectedRecordCount
                )
                && Objects.equals(
                        actualLoadedRecordCount,
                        that.actualLoadedRecordCount
                )
                && Objects.equals(startedAt, that.startedAt)
                && Objects.equals(completedAt, that.completedAt)
                && Objects.equals(failureReason, that.failureReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                executionId,
                sourceFile,
                status,
                expectedRecordCount,
                actualLoadedRecordCount,
                startedAt,
                completedAt,
                failureReason
        );
    }

    @Override
    public String toString() {
        return "IngestionExecution{"
                + "executionId='" + executionId + '\''
                + ", sourceFile='" + sourceFile + '\''
                + ", status='" + status + '\''
                + ", expectedRecordCount="
                + expectedRecordCount
                + ", actualLoadedRecordCount="
                + actualLoadedRecordCount
                + ", startedAt=" + startedAt
                + ", completedAt=" + completedAt
                + ", failureReason='" + failureReason + '\''
                + '}';
    }
}
