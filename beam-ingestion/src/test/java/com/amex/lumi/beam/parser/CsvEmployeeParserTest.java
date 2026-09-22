package com.amex.lumi.beam.parser;

import com.amex.lumi.beam.model.Address;
import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.model.EmergencyContact;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class CsvEmployeeParserTest {

    private final CsvEmployeeParser parser = new CsvEmployeeParser();

    @Test
    void shouldParseCompleteEmployeeRecord() throws IOException {

        String csv = """
                employee_id,first_name,last_name,email,phone_number,hire_date,department,job_title,salary,currency,employment_status,manager_id,is_active,skills,address_street,address_city,address_state,address_postal_code,address_country,emergency_contact_name,emergency_contact_relationship,emergency_contact_phone,emergency_contact_email
                EMP0001,Ravi,Kumar,ravi@example.com,9876543210,2025-01-15,Engineering,Software Engineer,75000,USD,ACTIVE,MGR001,true,Java;Spring Boot;PostgreSQL,123 Main Street,Indore,MP,452001,India,Anita Kumar,Sister,9123456789,anita@example.com
                """;

        CSVRecord record = parseSingleRecord(csv);

        EmployeeRecord employee = parser.parse(record);

        assertEquals("EMP0001", employee.getEmployeeId());
        assertEquals("Ravi", employee.getFirstName());
        assertEquals("Kumar", employee.getLastName());
        assertEquals("ravi@example.com", employee.getEmail());
        assertEquals("9876543210", employee.getPhoneNumber());
        assertEquals("2025-01-15", employee.getHireDate());
        assertEquals("Engineering", employee.getDepartment());
        assertEquals("Software Engineer", employee.getJobTitle());
        assertEquals(75000L, employee.getSalary());
        assertEquals("USD", employee.getCurrency());
        assertEquals("ACTIVE", employee.getEmploymentStatus());
        assertEquals("MGR001", employee.getManagerId());
        assertTrue(employee.getIsActive());

        assertEquals(
                java.util.List.of(
                        "Java",
                        "Spring Boot",
                        "PostgreSQL"
                ),
                employee.getSkills()
        );

        Address address = employee.getAddress();

        assertNotNull(address);
        assertEquals("123 Main Street", address.getStreet());
        assertEquals("Indore", address.getCity());
        assertEquals("MP", address.getState());
        assertEquals("452001", address.getPostalCode());
        assertEquals("India", address.getCountry());

        EmergencyContact emergencyContact =
                employee.getEmergencyContact();

        assertNotNull(emergencyContact);
        assertEquals("Anita Kumar", emergencyContact.getName());
        assertEquals("Sister", emergencyContact.getRelationship());
        assertEquals("9123456789", emergencyContact.getPhone());
        assertEquals(
                "anita@example.com",
                emergencyContact.getEmail()
        );
    }

    @Test
    void shouldParseEmptyOptionalFields() throws IOException {

        String csv = """
                employee_id,first_name,last_name,email,phone_number,hire_date,department,job_title,salary,currency,employment_status,manager_id,is_active,skills,address_street,address_city,address_state,address_postal_code,address_country,emergency_contact_name,emergency_contact_relationship,emergency_contact_phone,emergency_contact_email
                EMP0002,Ravi,,ravi@example.com,9876543210,2025-01-15,,,75000,USD,,MGR001,true,,,,,,,,,,
                """;

        CSVRecord record = parseSingleRecord(csv);

        EmployeeRecord employee = parser.parse(record);

        assertEquals("EMP0002", employee.getEmployeeId());
        assertEquals("Ravi", employee.getFirstName());

        assertNull(employee.getLastName());
        assertNull(employee.getDepartment());
        assertNull(employee.getJobTitle());
        assertNull(employee.getEmploymentStatus());

        assertEquals(75000L, employee.getSalary());
        assertTrue(employee.getIsActive());

        assertNotNull(employee.getSkills());
        assertTrue(employee.getSkills().isEmpty());

        assertNotNull(employee.getAddress());
        assertNotNull(employee.getEmergencyContact());
    }

    @Test
    void shouldParseSkillsWithSpaces() throws IOException {

        String csv = """
                employee_id,first_name,last_name,email,phone_number,hire_date,department,job_title,salary,currency,employment_status,manager_id,is_active,skills,address_street,address_city,address_state,address_postal_code,address_country,emergency_contact_name,emergency_contact_relationship,emergency_contact_phone,emergency_contact_email
                EMP0003,Ravi,Kumar,ravi@example.com,9876543210,2025-01-15,Engineering,Developer,80000,USD,ACTIVE,MGR001,true,"Java; Spring Boot ; PostgreSQL ",,,,,,,,,,
                """;

        CSVRecord record = parseSingleRecord(csv);

        EmployeeRecord employee = parser.parse(record);

        assertEquals(
                java.util.List.of(
                        "Java",
                        "Spring Boot",
                        "PostgreSQL"
                ),
                employee.getSkills()
        );
    }

   private CSVRecord parseSingleRecord(String csv)
        throws IOException {

    CSVFormat format = CSVFormat.DEFAULT
            .builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .build();

    CSVParser csvParser = CSVParser.parse(csv, format);

    return csvParser.iterator().next();
}

}
