package com.amex.lumi.beam.validate;

import com.amex.lumi.beam.TestEmployees;
import com.amex.lumi.beam.model.EmployeeRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmployeeValidatorTest {

    private static final String CURRENCY_ERROR = "currency must be an ISO 4217 code such as INR or USD";
    private static final String STATUS_ERROR =
            "employment_status must be one of: Contract, Full-time, Intern, Part-time, Temporary";

    private final EmployeeValidator validator = new EmployeeValidator();

    @Test
    void validEmployeeHasNoErrors() {
        assertTrue(validator.validate(TestEmployees.valid()).isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"EMP001", "EMP00001"})
    void employeeIdMustBeExactlySevenCharacters(String id) {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().employeeId(id).build();
        assertEquals(List.of("employee_id must be exactly 7 characters (found " + id.length() + ")"),
                validator.validate(employee));
    }

    @Test
    void firstNameNeedsAtLeastThreeCharacters() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().firstName("Al").build();
        assertEquals(List.of("first_name must be between 3 and 15 characters (found 2)"),
                validator.validate(employee));
    }

    @Test
    void lastNameIsOptional() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().lastName(null).build();
        assertTrue(validator.validate(employee).isEmpty());
    }

    @Test
    void phoneMustBeExactlyTenCharacters() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().phoneNumber("98765432101").build();
        assertEquals(List.of("phone_number must be exactly 10 characters (found 11)"),
                validator.validate(employee));
    }

    @Test
    void emailMustBeLongEnoughAndWellFormed() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().email("a@b.co").build();
        assertEquals(List.of("email must be between 13 and 30 characters (found 6)"), validator.validate(employee));

        employee = employee.toBuilder().email("no-at-sign.example.com").build();
        assertEquals(List.of("email has an invalid format"), validator.validate(employee));
    }

    @Test
    void hireDateMustBeARealDate() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().hireDate("2022-02-30").build();
        assertEquals(List.of("hire_date must be a real date in yyyy-MM-dd format"), validator.validate(employee));
    }

    @Test
    void currencyMustBeARealIsoCode() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().currency("inr").build();
        assertEquals(List.of(CURRENCY_ERROR), validator.validate(employee));
        employee = employee.toBuilder().currency("XYZ").build();
        assertEquals(List.of(CURRENCY_ERROR), validator.validate(employee));
        employee = employee.toBuilder().currency("USD").build();
        assertEquals(List.of(), validator.validate(employee));
    }

    @Test
    void skillsMustNotExceedOneHundredCharacters() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().skills(Collections.nCopies(20, "Kubernetes")).build();
        assertEquals(List.of("skills must not exceed 100 characters in total"), validator.validate(employee));
    }

    @Test
    void reportsEveryProblemAtOnce() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().employeeId(null).build();
        employee = employee.toBuilder().managerId(null).build();
        employee = employee.toBuilder().isActive(null).build();
        assertEquals(List.of("employee_id is required", "manager_id is required",
                "is_active is required (true or false)"), validator.validate(employee));
    }

    @Test
    void negativeSalaryIsRejected() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().salary(-1L).build();
        assertEquals(List.of("salary must not be negative"), validator.validate(employee));
    }

    @Test
    void departmentLongerThanTwentyIsRejected() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().department("Research and Development").build();
        assertEquals(List.of("department must be between 0 and 20 characters (found 24)"),
                validator.validate(employee));
    }

    @Test
    void employmentStatusNeedsThreeCharacters() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().employmentStatus("FT").build();
        assertEquals(List.of("employment_status must be between 3 and 13 characters (found 2)", STATUS_ERROR),
                validator.validate(employee));
    }

    @Test
    void employmentStatusMustBeAKnownValue() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().employmentStatus("whatever").build();
        assertEquals(List.of(STATUS_ERROR), validator.validate(employee));
    }

    @ParameterizedTest
    @ValueSource(strings = {"MGR01", "MGR000001"})
    void managerIdMustBeExactlySevenCharacters(String managerId) {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().managerId(managerId).build();
        assertEquals(List.of("manager_id must be exactly 7 characters (found " + managerId.length() + ")"),
                validator.validate(employee));
    }

    @Test
    void emailLongerThanThirtyIsRejected() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().email("a.very.long.email.address@techcorp.com").build();
        assertEquals(List.of("email must be between 13 and 30 characters (found 38)"), validator.validate(employee));
    }

    @Test
    void blankRequiredValueCountsAsMissing() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().firstName("   ").build();
        assertEquals(List.of("first_name is required"), validator.validate(employee));
    }

    @Test
    void phoneMustBeDigitsOnly() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().phoneNumber("abcdefghij").build();
        assertEquals(List.of("phone_number must contain digits only"), validator.validate(employee));
    }

    @Test
    void hireDateMustNotBeInTheFuture() {
        Clock today = Clock.fixed(Instant.parse("2026-09-28T10:00:00Z"), ZoneOffset.UTC);
        EmployeeRecord employee = TestEmployees.valid();

        employee = employee.toBuilder().hireDate("2026-09-28").build();
        assertEquals(List.of(), new EmployeeValidator(today).validate(employee));
        employee = employee.toBuilder().hireDate("2026-09-29").build();
        assertEquals(List.of("hire_date must not be in the future"), new EmployeeValidator(today).validate(employee));
    }

    @Test
    void idsMustBeLettersAndDigitsOnly() {
        EmployeeRecord employee = TestEmployees.valid();
        employee = employee.toBuilder().employeeId("EMP-001").build();
        employee = employee.toBuilder().managerId("MGR 001").build();
        assertEquals(List.of("employee_id must contain only letters and digits",
                "manager_id must contain only letters and digits"), validator.validate(employee));
    }
}
