package com.amex.lumi.beam.parser;

import com.amex.lumi.beam.model.Address;
import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.model.EmergencyContact;
import org.apache.commons.csv.CSVRecord;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Maps named CSV columns to the employee domain model.
 *
 * <p>Optional scalar values remain {@code null}; missing list values become
 * empty lists so downstream validation can apply the business rules.</p>
 */
public class CsvEmployeeParser {

        /**
         * Parses one header-mapped CSV record.
         *
         * @param record CSV record supplied by Apache Commons CSV
         * @return populated employee model
         */
    public EmployeeRecord parse(CSVRecord record) {

        EmployeeRecord employee = new EmployeeRecord();

        employee.setEmployeeId(getValue(record, "employee_id"));
        employee.setFirstName(getValue(record, "first_name"));
        employee.setLastName(getValue(record, "last_name"));
        employee.setEmail(getValue(record, "email"));
        employee.setPhoneNumber(getValue(record, "phone_number"));
        employee.setHireDate(getValue(record, "hire_date"));
        employee.setDepartment(getValue(record, "department"));
        employee.setJobTitle(getValue(record, "job_title"));

        employee.setSalary(parseSalary(record));

        employee.setCurrency(getValue(record, "currency"));
        employee.setEmploymentStatus(
                getValue(record, "employment_status")
        );
        employee.setManagerId(getValue(record, "manager_id"));

        employee.setIsActive(parseBoolean(record));

        employee.setSkills(parseSkills(record));

        employee.setAddress(parseAddress(record));

        employee.setEmergencyContact(
                parseEmergencyContact(record)
        );

        return employee;
    }

    private String getValue(CSVRecord record, String columnName) {
        if (!record.isMapped(columnName)) {
            return null;
        }

        String value = record.get(columnName);

        if (value == null) {
            return null;
        }

        value = value.trim();

        return value.isEmpty() ? null : value;
    }

    private Long parseSalary(CSVRecord record) {

        String salary = getValue(record, "salary");

        if (salary == null) {
            return null;
        }

        return Long.valueOf(salary);
    }

    private Boolean parseBoolean(CSVRecord record) {

        String value = getValue(record, "is_active");

        if (value == null) {
            return null;
        }

        return Boolean.valueOf(value);
    }

    private List<String> parseSkills(CSVRecord record) {

        String skills = getValue(record, "skills");

        if (skills == null) {
            return Collections.emptyList();
        }

        return Arrays.stream(skills.split(";"))
                .map(String::trim)
                .filter(skill -> !skill.isEmpty())
                .collect(Collectors.toList());
    }

    private Address parseAddress(CSVRecord record) {

        Address address = new Address();

        address.setStreet(
                getValue(record, "address_street")
        );

        address.setCity(
                getValue(record, "address_city")
        );

        address.setState(
                getValue(record, "address_state")
        );

        address.setPostalCode(
                getValue(record, "address_postal_code")
        );

        address.setCountry(
                getValue(record, "address_country")
        );

        return address;
    }

    private EmergencyContact parseEmergencyContact(
            CSVRecord record) {

        EmergencyContact emergencyContact =
                new EmergencyContact();

        emergencyContact.setName(
                getValue(record, "emergency_contact_name")
        );

        emergencyContact.setRelationship(
                getValue(record, "emergency_contact_relationship")
        );

        emergencyContact.setPhone(
                getValue(record, "emergency_contact_phone")
        );

        emergencyContact.setEmail(
                getValue(record, "emergency_contact_email")
        );

        return emergencyContact;
    }
}
