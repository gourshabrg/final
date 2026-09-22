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

    /** Matches CSV_ROW. */
    public static EmployeeRecord valid() {
        EmployeeRecord employee = new EmployeeRecord();
        employee.setEmployeeId("EMP0001");
        employee.setFirstName("Arjun");
        employee.setLastName("Sharma");
        employee.setEmail("arjun.sharma@techcorp.com");
        employee.setPhoneNumber("9876543210");
        employee.setHireDate("2022-03-15");
        employee.setDepartment("Engineering");
        employee.setJobTitle("Senior Backend Engineer");
        employee.setSalary(950000L);
        employee.setCurrency("INR");
        employee.setEmploymentStatus("Full-time");
        employee.setManagerId("MGR0001");
        employee.setIsActive(true);
        employee.setSkills(List.of("Python", "Docker"));

        Address address = new Address();
        address.setStreet("102, Silicon Heights");
        address.setCity("Bengaluru");
        address.setState("Karnataka");
        address.setPostalCode("560100");
        address.setCountry("India");
        employee.setAddress(address);

        EmergencyContact contact = new EmergencyContact();
        contact.setName("Priya Sharma");
        contact.setRelationship("Spouse");
        contact.setPhone("9876543211");
        contact.setEmail("priya.s@example.com");
        employee.setEmergencyContact(contact);
        return employee;
    }
}
