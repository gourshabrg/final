package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.dto.EmployeeResponse;
import com.amex.lumi.ingestion.exception.DecryptionException;
import com.amex.lumi.ingestion.exception.ResourceNotFoundException;
import com.amex.lumi.ingestion.exception.ServerDataException;
import com.amex.lumi.ingestion.repository.EmployeeRepository;
import com.amex.lumi.ingestion.repository.EmployeeRow;
import com.amex.lumi.ingestion.security.FieldDecryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

/**
 * Loads an employee and decrypts phone, salary and emergency contact phone.
 */
@Service
public class EmployeeService {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmployeeService.class);

    private final EmployeeRepository repository;
    private final FieldDecryptor decryptor;
    private final JsonMapper jsonMapper;

    public EmployeeService(EmployeeRepository repository, FieldDecryptor decryptor, JsonMapper jsonMapper) {
        this.repository = repository;
        this.decryptor = decryptor;
        this.jsonMapper = jsonMapper;
    }

    public EmployeeResponse getDecryptedEmployee(String employeeId) {
        EmployeeRow row = repository.findById(employeeId)
                .orElseThrow(() -> new ResourceNotFoundException("Employee not found: " + employeeId));
        try {
            EmployeeResponse response = toResponse(row);
            // Audit log: who was decrypted, never the values.
            LOGGER.info("Decrypted sensitive fields for employee_id={}", employeeId);
            return response;
        } catch (DecryptionException | JacksonException | NumberFormatException exception) {
            // The stored row is broken (wrong key, bad JSON), not the request: a server error, not a 400.
            throw new ServerDataException("Stored data for employee " + employeeId + " could not be read", exception);
        }
    }

    private EmployeeResponse toResponse(EmployeeRow row) {
        EmployeeResponse.EmergencyContact contact = readJson(row.emergencyContactJson(),
                EmployeeResponse.EmergencyContact.class);
        if (contact != null) {
            contact = contact.withPhone(decryptor.decryptIfPresent(contact.phone()));
        }
        String salary = decryptor.decryptIfPresent(row.salaryEncrypted());
        return new EmployeeResponse(
                row.employeeId(), row.firstName(), row.lastName(), row.email(),
                decryptor.decryptIfPresent(row.phoneNumberEncrypted()),
                row.hireDate(), row.department(), row.jobTitle(),
                salary == null ? null : Long.valueOf(salary),
                row.currency(), row.employmentStatus(), row.managerId(), row.isActive(),
                row.skillsJson() == null ? List.of() : jsonMapper.readValue(row.skillsJson(), new TypeReference<>() {
                }),
                readJson(row.addressJson(), EmployeeResponse.Address.class),
                contact,
                row.ingestionTimestamp(), row.executionId(), row.sourceCreationTime());
    }

    private <T> T readJson(String json, Class<T> type) {
        return json == null ? null : jsonMapper.readValue(json, type);
    }
}
