package com.amex.lumi.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * An employee with phone, salary and emergency phone decrypted.
 */
public record EmployeeResponse(
        @JsonProperty("employee_id") String employeeId,
        @JsonProperty("first_name") String firstName,
        @JsonProperty("last_name") String lastName,
        String email,
        @JsonProperty("phone_number") String phoneNumber,
        @JsonProperty("hire_date") LocalDate hireDate,
        String department,
        @JsonProperty("job_title") String jobTitle,
        Long salary,
        String currency,
        @JsonProperty("employment_status") String employmentStatus,
        @JsonProperty("manager_id") String managerId,
        @JsonProperty("is_active") Boolean isActive,
        List<String> skills,
        Address address,
        @JsonProperty("emergency_contact") EmergencyContact emergencyContact,
        @JsonProperty("ingestion_timestamp") Instant ingestionTimestamp,
        @JsonProperty("execution_id") UUID executionId,
        @JsonProperty("source_creation_time") Instant sourceCreationTime) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Address(
            String street,
            String city,
            String state,
            @JsonProperty("postal_code") String postalCode,
            String country) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record EmergencyContact(
            String name,
            String relationship,
            String phone,
            String email) {

        public EmergencyContact withPhone(String newPhone) {
            return new EmergencyContact(name, relationship, newPhone, email);
        }
    }
}
