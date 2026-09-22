package com.amex.lumi.beam.model;

import java.io.Serializable;

/**
 * An employee ready to save. Phone, salary and emergency phone are saved from the encrypted fields, never the plain ones.
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

    public String executionId() {
        return enriched.executionId();
    }
}
