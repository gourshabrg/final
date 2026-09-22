package com.amex.lumi.beam.read;

import com.amex.lumi.beam.common.PipelineConstants;
import com.amex.lumi.beam.model.Address;
import com.amex.lumi.beam.model.EmergencyContact;
import com.amex.lumi.beam.model.EmployeeRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Reads CSV files that have a header row. Commons CSV correctly handles values with commas in quotes, like "102, Main Road".
 */
public class CsvEmployeeParser implements EmployeeFileParser {

    private static final long serialVersionUID = 1L;

    private static final CSVFormat FORMAT = CSVFormat.DEFAULT.builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .setIgnoreEmptyLines(true)
            .setTrim(true)
            .get();

    @Override
    public void parse(Reader reader, RecordHandler handler) throws IOException {
        try (CSVParser csv = FORMAT.parse(reader)) {
            for (CSVRecord row : csv) {
                if (!row.isConsistent()) {
                    handler.onError("row has " + row.size() + " column(s) but the header has "
                            + csv.getHeaderNames().size());
                    continue;
                }
                try {
                    handler.onRecord(toEmployee(row));
                } catch (IllegalArgumentException exception) {
                    handler.onError(exception.getMessage());
                }
            }
        }
    }

    private static EmployeeRecord toEmployee(CSVRecord row) {
        EmployeeRecord employee = new EmployeeRecord();
        employee.setEmployeeId(value(row, "employee_id"));
        employee.setFirstName(value(row, "first_name"));
        employee.setLastName(value(row, "last_name"));
        employee.setEmail(value(row, "email"));
        employee.setPhoneNumber(value(row, "phone_number"));
        employee.setHireDate(value(row, "hire_date"));
        employee.setDepartment(value(row, "department"));
        employee.setJobTitle(value(row, "job_title"));
        employee.setSalary(toSalary(value(row, "salary")));
        employee.setCurrency(value(row, "currency"));
        employee.setEmploymentStatus(value(row, "employment_status"));
        employee.setManagerId(value(row, "manager_id"));
        employee.setIsActive(toBoolean(value(row, "is_active")));
        employee.setSkills(toSkills(value(row, "skills")));

        Address address = new Address();
        address.setStreet(value(row, "address_street"));
        address.setCity(value(row, "address_city"));
        address.setState(value(row, "address_state"));
        address.setPostalCode(value(row, "address_postal_code"));
        address.setCountry(value(row, "address_country"));
        employee.setAddress(address);

        EmergencyContact contact = new EmergencyContact();
        contact.setName(value(row, "emergency_contact_name"));
        contact.setRelationship(value(row, "emergency_contact_relationship"));
        contact.setPhone(value(row, "emergency_contact_phone"));
        contact.setEmail(value(row, "emergency_contact_email"));
        employee.setEmergencyContact(contact);

        return employee;
    }

    // Missing column or empty cell -> null.
    private static String value(CSVRecord row, String column) {
        if (!row.isMapped(column)) {
            return null;
        }
        String raw = row.get(column);
        return raw == null || raw.isBlank() ? null : raw.trim();
    }

    private static Long toSalary(String raw) {
        if (raw == null) {
            return null;
        }
        try {
            return Long.valueOf(raw);
        } catch (NumberFormatException exception) {
            // Value left out: salary is sensitive.
            throw new IllegalArgumentException("salary must be a whole number");
        }
    }

    // Boolean.valueOf("yes") would silently give false.
    private static Boolean toBoolean(String raw) {
        if (raw == null) {
            return null;
        }
        return switch (raw.toLowerCase(Locale.ROOT)) {
            case "true" -> Boolean.TRUE;
            case "false" -> Boolean.FALSE;
            default -> throw new IllegalArgumentException("is_active must be true or false");
        };
    }

    private static List<String> toSkills(String raw) {
        if (raw == null) {
            return List.of();
        }
        return Arrays.stream(raw.split(PipelineConstants.SKILL_SEPARATOR))
                .map(String::trim)
                .filter(skill -> !skill.isEmpty())
                .toList();
    }
}
