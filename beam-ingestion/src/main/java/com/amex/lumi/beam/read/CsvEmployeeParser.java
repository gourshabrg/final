package com.amex.lumi.beam.read;

import com.amex.lumi.beam.common.PipelineConstants;
import com.amex.lumi.beam.model.Address;
import com.amex.lumi.beam.model.EmergencyContact;
import com.amex.lumi.beam.model.EmployeeRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Reads CSV with a header row; Commons CSV handles quoted commas.
 */
public class CsvEmployeeParser implements EmployeeFileParser {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(CsvEmployeeParser.class);

    // Without one of these, every row would fail with the same "is required" error.
    static final List<String> REQUIRED_COLUMNS = List.of("employee_id", "first_name", "email", "phone_number",
            "hire_date", "currency", "employment_status", "manager_id", "is_active");
    private static final Set<String> KNOWN_COLUMNS = Set.of("employee_id", "first_name", "last_name", "email",
            "phone_number", "hire_date", "department", "job_title", "salary", "currency", "employment_status",
            "manager_id", "is_active", "skills", "address_street", "address_city", "address_state",
            "address_postal_code", "address_country", "emergency_contact_name", "emergency_contact_relationship",
            "emergency_contact_phone", "emergency_contact_email",
            PipelineConstants.SOURCE_RECORD_NUMBER, PipelineConstants.CORRUPT_RECORD);

    // Spark pads or cuts such a row to the header size, so the real column count is no longer known.
    static final String SPARK_CORRUPT_ROW = "row has a different number of columns than the header";

    private static final CSVFormat FORMAT = CSVFormat.DEFAULT.builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .setIgnoreEmptyLines(true)
            .setTrim(true)
            .get();

    @Override
    public void parse(Reader reader, RecordHandler handler) throws IOException {
        try (CSVParser csv = FORMAT.parse(reader)) {
            if (!headerIsUsable(csv.getHeaderNames(), handler)) {
                return;
            }
            for (CSVRecord row : csv) {
                Long sourceRecordNumber = sourceRecordNumber(row);
                if (!row.isConsistent()) {
                    handler.onError("row has " + row.size() + " column(s) but the header has "
                            + csv.getHeaderNames().size(), sourceRecordNumber);
                    continue;
                }
                if (value(row, PipelineConstants.CORRUPT_RECORD) != null) {
                    handler.onError(SPARK_CORRUPT_ROW, sourceRecordNumber);
                    continue;
                }
                try {
                    handler.onRecord(toEmployee(row), sourceRecordNumber);
                } catch (IllegalArgumentException exception) {
                    handler.onError(exception.getMessage(), sourceRecordNumber);
                }
            }
        }
    }

    private static boolean headerIsUsable(List<String> header, RecordHandler handler) {
        List<String> missing = REQUIRED_COLUMNS.stream().filter(column -> !header.contains(column)).toList();
        if (!missing.isEmpty()) {
            handler.onFileError("CSV header is missing required column(s): " + String.join(", ", missing));
            return false;
        }
        List<String> unknown = header.stream().filter(column -> !KNOWN_COLUMNS.contains(column)).toList();
        if (!unknown.isEmpty()) {
            LOGGER.warn("Ignoring unknown CSV column(s): {}", unknown);
        }
        return true;
    }

    // Only split files have this column; a broken row may be too short to reach it.
    private static Long sourceRecordNumber(CSVRecord row) {
        Integer index = row.getParser().getHeaderMap().get(PipelineConstants.SOURCE_RECORD_NUMBER);
        if (index == null || index >= row.size()) {
            return null;
        }
        try {
            return Long.valueOf(row.get(index).trim());
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private static EmployeeRecord toEmployee(CSVRecord row) {
        return EmployeeRecord.builder()
                .employeeId(value(row, "employee_id"))
                .firstName(value(row, "first_name"))
                .lastName(value(row, "last_name"))
                .email(value(row, "email"))
                .phoneNumber(value(row, "phone_number"))
                .hireDate(value(row, "hire_date"))
                .department(value(row, "department"))
                .jobTitle(value(row, "job_title"))
                .salary(toSalary(value(row, "salary")))
                .currency(value(row, "currency"))
                .employmentStatus(value(row, "employment_status"))
                .managerId(value(row, "manager_id"))
                .isActive(toBoolean(value(row, "is_active")))
                .skills(toSkills(value(row, "skills")))
                .address(Address.builder()
                        .street(value(row, "address_street"))
                        .city(value(row, "address_city"))
                        .state(value(row, "address_state"))
                        .postalCode(value(row, "address_postal_code"))
                        .country(value(row, "address_country"))
                        .build())
                .emergencyContact(EmergencyContact.builder()
                        .name(value(row, "emergency_contact_name"))
                        .relationship(value(row, "emergency_contact_relationship"))
                        .phone(value(row, "emergency_contact_phone"))
                        .email(value(row, "emergency_contact_email"))
                        .build())
                .build();
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
