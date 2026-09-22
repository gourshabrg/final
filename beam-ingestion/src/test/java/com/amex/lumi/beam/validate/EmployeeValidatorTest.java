package com.amex.lumi.beam.validate;

import com.amex.lumi.beam.TestEmployees;
import com.amex.lumi.beam.model.EmployeeRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmployeeValidatorTest {

    private final EmployeeValidator validator = new EmployeeValidator();

    @Test
    void validEmployeeHasNoErrors() {
        assertTrue(validator.validate(TestEmployees.valid()).isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"EMP001", "EMP00001"})
    void employeeIdMustBeExactlySevenCharacters(String id) {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setEmployeeId(id);
        assertEquals(List.of("employee_id must be exactly 7 characters (found " + id.length() + ")"),
                validator.validate(employee));
    }

    @Test
    void firstNameNeedsAtLeastThreeCharacters() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setFirstName("Al");
        assertEquals(List.of("first_name must be between 3 and 15 characters (found 2)"),
                validator.validate(employee));
    }

    @Test
    void lastNameIsOptional() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setLastName(null);
        assertTrue(validator.validate(employee).isEmpty());
    }

    @Test
    void phoneMustBeExactlyTenCharacters() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setPhoneNumber("+91-98765-43210");
        assertEquals(List.of("phone_number must be exactly 10 characters (found 15)"),
                validator.validate(employee));
    }

    @Test
    void emailMustBeLongEnoughAndWellFormed() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setEmail("a@b.co");
        assertEquals(List.of("email must be between 13 and 30 characters (found 6)"), validator.validate(employee));

        employee.setEmail("no-at-sign.example.com");
        assertEquals(List.of("email has an invalid format"), validator.validate(employee));
    }

    @Test
    void hireDateMustBeARealDate() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setHireDate("2022-02-30");
        assertEquals(List.of("hire_date must be a real date in yyyy-MM-dd format"), validator.validate(employee));
    }

    @Test
    void currencyMustBeThreeUppercaseLetters() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setCurrency("inr");
        assertEquals(List.of("currency must be a 3-letter uppercase ISO code such as INR or USD"),
                validator.validate(employee));
    }

    @Test
    void skillsMustNotExceedOneHundredCharacters() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setSkills(Collections.nCopies(20, "Kubernetes"));
        assertEquals(List.of("skills must not exceed 100 characters in total"), validator.validate(employee));
    }

    @Test
    void reportsEveryProblemAtOnce() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setEmployeeId(null);
        employee.setManagerId(null);
        employee.setIsActive(null);
        assertEquals(List.of("employee_id is required", "manager_id is required",
                "is_active is required (true or false)"), validator.validate(employee));
    }

    @Test
    void negativeSalaryIsRejected() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setSalary(-1L);
        assertEquals(List.of("salary must not be negative"), validator.validate(employee));
    }

    @Test
    void departmentLongerThanTwentyIsRejected() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setDepartment("Research and Development");
        assertEquals(List.of("department must be between 0 and 20 characters (found 24)"),
                validator.validate(employee));
    }

    @Test
    void employmentStatusNeedsThreeCharacters() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setEmploymentStatus("FT");
        assertEquals(List.of("employment_status must be between 3 and 13 characters (found 2)"),
                validator.validate(employee));
    }

    @ParameterizedTest
    @ValueSource(strings = {"MGR01", "MGR000001"})
    void managerIdMustBeExactlySevenCharacters(String managerId) {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setManagerId(managerId);
        assertEquals(List.of("manager_id must be exactly 7 characters (found " + managerId.length() + ")"),
                validator.validate(employee));
    }

    @Test
    void emailLongerThanThirtyIsRejected() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setEmail("a.very.long.email.address@techcorp.com");
        assertEquals(List.of("email must be between 13 and 30 characters (found 38)"), validator.validate(employee));
    }

    @Test
    void blankRequiredValueCountsAsMissing() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setFirstName("   ");
        assertEquals(List.of("first_name is required"), validator.validate(employee));
    }
}
