package com.amex.lumi.beam.sink;

import com.amex.lumi.beam.model.EmployeeRecord;

import java.io.Serializable;
import java.util.Objects;

/**
 * Failure details produced when a validated employee cannot be persisted.
 */
public class LoadFailure implements Serializable {

    private static final long serialVersionUID = 1L;

    private final EmployeeRecord employee;
    private final long recordNumber;
    private final String sourceFile;
    private final String errorType;
    private final String errorMessage;
    private final String executionId;

    public LoadFailure(
            EmployeeRecord employee,
            long recordNumber,
            String sourceFile,
            String errorType,
            String errorMessage,
            String executionId) {

        this.employee = employee;
        this.recordNumber = recordNumber;
        this.sourceFile = sourceFile;
        this.errorType = errorType;
        this.errorMessage = errorMessage;
        this.executionId = executionId;
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

    public String getErrorType() {
        return errorType;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getExecutionId() {
        return executionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof LoadFailure that)) {
            return false;
        }

        return recordNumber == that.recordNumber
                && Objects.equals(employee, that.employee)
                && Objects.equals(sourceFile, that.sourceFile)
                && Objects.equals(errorType, that.errorType)
                && Objects.equals(errorMessage, that.errorMessage)
                && Objects.equals(executionId, that.executionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                employee,
                recordNumber,
                sourceFile,
                errorType,
                errorMessage,
                executionId
        );
    }
}
