package com.amex.lumi.beam.model;

import java.io.Serializable;

/**
 * A rejected record (parse, validation or load error). Employee is null for parse errors.
 */
public record RecordFailure(
        long recordNumber,
        String sourceFile,
        String executionId,
        FailureType type,
        String message,
        EmployeeRecord employee) implements Serializable {

    public enum FailureType {
        PARSE_ERROR,
        VALIDATION_ERROR,
        LOAD_ERROR
    }

    public String employeeId() {
        return employee == null ? null : employee.getEmployeeId();
    }
}
