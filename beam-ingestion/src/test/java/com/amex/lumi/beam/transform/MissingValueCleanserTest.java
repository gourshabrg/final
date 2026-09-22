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
        employee.setLastName(null);
        employee.setDepartment("");
        employee.setAddress(null);
        employee.setSkills(null);

        EmployeeRecord cleaned = MissingValueCleanser.cleanse(employee);

        assertEquals(" ", cleaned.getLastName());
        assertEquals(" ", cleaned.getDepartment());
        assertEquals(" ", cleaned.getAddress().getCity());
        assertEquals(List.of(), cleaned.getSkills());
    }

    @Test
    void leavesNonTextFieldsAndOriginalUntouched() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setSalary(null);
        employee.setLastName(null);

        EmployeeRecord cleaned = MissingValueCleanser.cleanse(employee);

        assertNull(cleaned.getSalary());
        assertNull(employee.getLastName());
        assertEquals("Arjun", cleaned.getFirstName());
    }

    @Test
    void missingEmergencyContactGetsWhitespaceFields() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setEmergencyContact(null);

        EmployeeRecord cleaned = MissingValueCleanser.cleanse(employee);

        assertEquals(" ", cleaned.getEmergencyContact().getName());
        assertEquals(" ", cleaned.getEmergencyContact().getPhone());
    }
}
