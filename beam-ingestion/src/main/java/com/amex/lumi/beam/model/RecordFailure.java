package com.amex.lumi.beam.model;

import java.io.Serializable;

/**
 * A rejected record; employee is null for parse errors, splitFile null when not split.
 */
public record RecordFailure(
        long recordNumber,
        String sourceFile,
        String splitFile,
        String executionId,
        FailureType type,
        String message,
        EmployeeRecord employee) implements Serializable {

    public enum FailureType {
        PARSE_ERROR,
        VALIDATION_ERROR,
        DUPLICATE_ERROR,
        LOAD_ERROR,
        STALE_ERROR
    }

    /** File was not split, so there is no part file. */
    public RecordFailure(long recordNumber, String sourceFile, String executionId, FailureType type,
                         String message, EmployeeRecord employee) {
        this(recordNumber, sourceFile, null, executionId, type, message, employee);
    }

    public String employeeId() {
        return employee == null ? null : employee.getEmployeeId();
    }
}
