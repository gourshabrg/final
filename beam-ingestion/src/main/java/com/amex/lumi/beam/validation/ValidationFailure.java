package com.amex.lumi.beam.validation;

import com.amex.lumi.beam.model.EmployeeRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Invalid employee record and the field-level validation errors for it.
 */
public class ValidationFailure implements Serializable {

    private static final long serialVersionUID = 1L;

    private final EmployeeRecord employee;
    private final long recordNumber;
    private final List<String> errors;

    /*
     * Metadata required for operational error tracking.
     */
    private final String sourceFile;
    private final String executionId;

    public ValidationFailure(
            EmployeeRecord employee,
            long recordNumber,
            List<String> errors,
            String sourceFile,
            String executionId) {

        this.employee = employee;
        this.recordNumber = recordNumber;
        this.errors = new ArrayList<>(errors);
        this.sourceFile = sourceFile;
        this.executionId = executionId;
    }

    public EmployeeRecord getEmployee() {
        return employee;
    }

    public long getRecordNumber() {
        return recordNumber;
    }

    public List<String> getErrors() {
        return errors;
    }

    public String getSourceFile() {
        return sourceFile;
    }

    public String getExecutionId() {
        return executionId;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }

        if (!(o instanceof ValidationFailure that)) {
            return false;
        }

        return recordNumber == that.recordNumber
                && Objects.equals(
                        employee,
                        that.employee
                )
                && Objects.equals(
                        errors,
                        that.errors
                )
                && Objects.equals(
                        sourceFile,
                        that.sourceFile
                )
                && Objects.equals(
                        executionId,
                        that.executionId
                );
    }

    @Override
    public int hashCode() {

        return Objects.hash(
                employee,
                recordNumber,
                errors,
                sourceFile,
                executionId
        );
    }
}
