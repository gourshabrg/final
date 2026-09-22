package com.amex.lumi.beam.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Employee payload plus immutable ingestion metadata required downstream.
 */
public class EnrichedEmployeeRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final EmployeeRecord employee;
    private final long recordNumber;
    private final String sourceFile;
    private final String executionId;
    private final Instant ingestionTimestamp;
    private final Instant sourceCreationTime;

    public EnrichedEmployeeRecord(
            EmployeeRecord employee,
            long recordNumber,
            String sourceFile,
            String executionId,
            Instant ingestionTimestamp,
            Instant sourceCreationTime) {

        this.employee = employee;
        this.recordNumber = recordNumber;
        this.sourceFile = sourceFile;
        this.executionId = executionId;
        this.ingestionTimestamp = ingestionTimestamp;
        this.sourceCreationTime = sourceCreationTime;
    }

    public EmployeeRecord getEmployee() {
        return employee;
    }

    public long getRecordNumber() {
        return recordNumber;
    }

    public String getSourceFile() {
        return sourceFile;
    }

    public String getExecutionId() {
        return executionId;
    }

    public Instant getIngestionTimestamp() {
        return ingestionTimestamp;
    }

    public Instant getSourceCreationTime() {
        return sourceCreationTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof EnrichedEmployeeRecord that)) {
            return false;
        }

        return recordNumber == that.recordNumber
                && Objects.equals(employee, that.employee)
                && Objects.equals(sourceFile, that.sourceFile)
                && Objects.equals(executionId, that.executionId)
                && Objects.equals(
                        ingestionTimestamp,
                        that.ingestionTimestamp
                )
                && Objects.equals(
                        sourceCreationTime,
                        that.sourceCreationTime
                );
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                employee,
                recordNumber,
                sourceFile,
                executionId,
                ingestionTimestamp,
                sourceCreationTime
        );
    }
}
