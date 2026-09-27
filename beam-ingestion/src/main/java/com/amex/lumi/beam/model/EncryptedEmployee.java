package com.amex.lumi.beam.model;

import java.io.Serializable;

/**
 * An employee ready to save; sensitive fields are saved only in encrypted form.
 */
public record EncryptedEmployee(
        EnrichedEmployee enriched,
        String encryptedPhoneNumber,
        String encryptedSalary,
        String encryptedEmergencyContactPhone) implements Serializable {

    public EmployeeRecord employee() {
        return enriched.employee();
    }

    public long recordNumber() {
        return enriched.parsed().recordNumber();
    }

    public String sourceFile() {
        return enriched.parsed().sourceFile();
    }

    public String splitFile() {
        return enriched.parsed().splitFile();
    }

    public String executionId() {
        return enriched.executionId();
    }
}
