package com.amex.lumi.ingestion.repository;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

/**
 * One employee row as stored: sensitive fields still encrypted, JSONB columns as text.
 */
public record EmployeeRow(
        String employeeId,
        String firstName,
        String lastName,
        String email,
        String phoneNumberEncrypted,
        LocalDate hireDate,
        String department,
        String jobTitle,
        String salaryEncrypted,
        String currency,
        String employmentStatus,
        String managerId,
        Boolean isActive,
        String skillsJson,
        String addressJson,
        String emergencyContactJson,
        Instant ingestionTimestamp,
        UUID executionId,
        Instant sourceCreationTime) {
}
