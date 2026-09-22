package com.amex.lumi.beam.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Enriched employee payload with sensitive fields prepared for persistence.
 */
public class EncryptedEmployeeRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final EmployeeRecord employee;
    private final String encryptedPhoneNumber;
    private final String encryptedSalary;
    private final String encryptedEmergencyContactPhone;

    private final long recordNumber;
    private final String sourceFile;

    private final String executionId;
    private final Instant ingestionTimestamp;
    private final Instant sourceCreationTime;

    public EncryptedEmployeeRecord(
            EmployeeRecord employee,
            String encryptedPhoneNumber,
            String encryptedSalary,
            String encryptedEmergencyContactPhone,
            long recordNumber,
            String sourceFile,
            String executionId,
            Instant ingestionTimestamp,
            Instant sourceCreationTime) {

        this.employee = employee;
        this.encryptedPhoneNumber = encryptedPhoneNumber;
        this.encryptedSalary = encryptedSalary;
        this.encryptedEmergencyContactPhone =
                encryptedEmergencyContactPhone;

        this.recordNumber = recordNumber;
        this.sourceFile = sourceFile;

        this.executionId = executionId;
        this.ingestionTimestamp = ingestionTimestamp;
        this.sourceCreationTime = sourceCreationTime;
    }

    public EmployeeRecord getEmployee() {
        return employee;
    }

    public String getEncryptedPhoneNumber() {
        return encryptedPhoneNumber;
    }

    public String getEncryptedSalary() {
        return encryptedSalary;
    }

    public String getEncryptedEmergencyContactPhone() {
        return encryptedEmergencyContactPhone;
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

        if (!(o instanceof EncryptedEmployeeRecord that)) {
            return false;
        }

        return recordNumber == that.recordNumber
                && Objects.equals(
                        employee,
                        that.employee
                )
                && Objects.equals(
                        encryptedPhoneNumber,
                        that.encryptedPhoneNumber
                )
                && Objects.equals(
                        encryptedSalary,
                        that.encryptedSalary
                )
                && Objects.equals(
                        encryptedEmergencyContactPhone,
                        that.encryptedEmergencyContactPhone
                )
                && Objects.equals(
                        sourceFile,
                        that.sourceFile
                )
                && Objects.equals(
                        executionId,
                        that.executionId
                )
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
                encryptedPhoneNumber,
                encryptedSalary,
                encryptedEmergencyContactPhone,
                recordNumber,
                sourceFile,
                executionId,
                ingestionTimestamp,
                sourceCreationTime
        );
    }
}
