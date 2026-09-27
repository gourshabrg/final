package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.TestEmployees;
import com.amex.lumi.beam.model.EmployeeRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MissingValueCleanserTest {

    @Test
    void replacesMissingTextWithWhitespace() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().lastName(null).build();
        employee = employee.toBuilder().department("").build();
        employee = employee.toBuilder().address(null).build();
        employee = employee.toBuilder().skills(null).build();

        EmployeeRecord cleaned = MissingValueCleanser.cleanse(employee);

        assertEquals(" ", cleaned.getLastName());
        assertEquals(" ", cleaned.getDepartment());
        assertEquals(" ", cleaned.getAddress().getCity());
        assertEquals(List.of(), cleaned.getSkills());
    }

    @Test
    void leavesNonTextFieldsAndOriginalUntouched() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().salary(null).build();
        employee = employee.toBuilder().lastName(null).build();

        EmployeeRecord cleaned = MissingValueCleanser.cleanse(employee);

        assertNull(cleaned.getSalary());
        assertNull(employee.getLastName());
        assertEquals("Arjun", cleaned.getFirstName());
    }

    @Test
    void missingEmergencyContactGetsWhitespaceFields() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().emergencyContact(null).build();

        EmployeeRecord cleaned = MissingValueCleanser.cleanse(employee);

        assertEquals(" ", cleaned.getEmergencyContact().getName());
        assertEquals(" ", cleaned.getEmergencyContact().getPhone());
    }
}
