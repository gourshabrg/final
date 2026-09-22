package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.config.EncryptionProperties;
import com.amex.lumi.ingestion.dto.EmployeeResponse;
import com.amex.lumi.ingestion.exception.ResourceNotFoundException;
import com.amex.lumi.ingestion.repository.EmployeeRepository;
import com.amex.lumi.ingestion.repository.EmployeeRow;
import com.amex.lumi.ingestion.security.FieldDecryptor;
import com.amex.lumi.ingestion.security.TestCipher;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EmployeeServiceTest {

    private final EmployeeRepository repository = mock(EmployeeRepository.class);
    private final EmployeeService service = new EmployeeService(repository,
            new FieldDecryptor(new EncryptionProperties(TestCipher.KEY)), JsonMapper.builder().build());

    private static EmployeeRow row(String phone, String salary, String skills, String address, String contact) {
        return new EmployeeRow("EMP0001", "Arjun", "Sharma", "arjun@techcorp.com", phone,
                LocalDate.of(2022, 3, 15), "Engineering", "Engineer", salary, "INR", "Full-time", "MGR0001",
                true, skills, address, contact, Instant.now(), UUID.randomUUID(), Instant.now());
    }

    @Test
    void decryptsSensitiveFields() {
        String contact = "{\"name\":\"Priya\",\"relationship\":\"Spouse\",\"phone\":\""
                + TestCipher.encrypt("9876543211") + "\",\"email\":\"p@example.com\"}";
        when(repository.findById("EMP0001")).thenReturn(Optional.of(row(TestCipher.encrypt("9876543210"),
                TestCipher.encrypt("950000"), "[\"Python\",\"Docker\"]",
                "{\"city\":\"Bengaluru\",\"postal_code\":\"560100\"}", contact)));

        EmployeeResponse employee = service.getDecryptedEmployee("EMP0001");

        assertThat(employee.phoneNumber()).isEqualTo("9876543210");
        assertThat(employee.salary()).isEqualTo(950000L);
        assertThat(employee.emergencyContact().phone()).isEqualTo("9876543211");
        assertThat(employee.skills()).isEqualTo(List.of("Python", "Docker"));
        assertThat(employee.address().postalCode()).isEqualTo("560100");
    }

    @Test
    void missingOptionalValuesStayEmpty() {
        when(repository.findById("EMP0001")).thenReturn(Optional.of(row(null, null, null, null, null)));

        EmployeeResponse employee = service.getDecryptedEmployee("EMP0001");

        assertThat(employee.phoneNumber()).isNull();
        assertThat(employee.salary()).isNull();
        assertThat(employee.skills()).isEmpty();
        assertThat(employee.address()).isNull();
        assertThat(employee.emergencyContact()).isNull();
    }

    @Test
    void unknownEmployeeIsNotFound() {
        when(repository.findById("NOPE000")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.getDecryptedEmployee("NOPE000"))
                .isInstanceOf(ResourceNotFoundException.class);
    }
}
