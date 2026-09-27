package com.amex.lumi.beam;

import com.amex.lumi.beam.model.Address;
import com.amex.lumi.beam.model.EmergencyContact;
import com.amex.lumi.beam.model.EmployeeRecord;

import java.util.List;

/** Test data shared by the unit tests. */
public final class TestEmployees {

    public static final String CSV_HEADER = "employee_id,first_name,last_name,email,phone_number,hire_date,"
            + "department,job_title,salary,currency,employment_status,manager_id,is_active,skills,"
            + "address_street,address_city,address_state,address_postal_code,address_country,"
            + "emergency_contact_name,emergency_contact_relationship,emergency_contact_phone,emergency_contact_email";

    public static final String CSV_ROW = "EMP0001,Arjun,Sharma,arjun.sharma@techcorp.com,9876543210,2022-03-15,"
            + "Engineering,Senior Backend Engineer,950000,INR,Full-time,MGR0001,true,Python;Docker,"
            + "\"102, Silicon Heights\",Bengaluru,Karnataka,560100,India,"
            + "Priya Sharma,Spouse,9876543211,priya.s@example.com";

    private TestEmployees() {
    }

    /** Matches CSV_ROW. Change a field with valid().toBuilder().firstName("x").build(). */
    public static EmployeeRecord valid() {
        return EmployeeRecord.builder()
                .employeeId("EMP0001")
                .firstName("Arjun")
                .lastName("Sharma")
                .email("arjun.sharma@techcorp.com")
                .phoneNumber("9876543210")
                .hireDate("2022-03-15")
                .department("Engineering")
                .jobTitle("Senior Backend Engineer")
                .salary(950000L)
                .currency("INR")
                .employmentStatus("Full-time")
                .managerId("MGR0001")
                .isActive(true)
                .skills(List.of("Python", "Docker"))
                .address(Address.builder()
                        .street("102, Silicon Heights")
                        .city("Bengaluru")
                        .state("Karnataka")
                        .postalCode("560100")
                        .country("India")
                        .build())
                .emergencyContact(EmergencyContact.builder()
                        .name("Priya Sharma")
                        .relationship("Spouse")
                        .phone("9876543211")
                        .email("priya.s@example.com")
                        .build())
                .build();
    }
}
