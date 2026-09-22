package com.amex.lumi.beam.write;

import com.amex.lumi.beam.model.EmergencyContact;
import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.model.EncryptedEmployee;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.util.UUID;

/**
 * Holds the SQL that saves an employee and fills in its values.
 */
final class EmployeeStatementBinder {

    // Upsert = insert, or update if the employee_id already exists. Re-running a file is then safe.
    static final String UPSERT_SQL = """
            INSERT INTO employee (
                employee_id, first_name, last_name, email, phone_number_encrypted,
                hire_date, department, job_title, salary_encrypted, currency,
                employment_status, manager_id, is_active, skills, address, emergency_contact,
                ingestion_timestamp, execution_id, source_creation_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (employee_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                phone_number_encrypted = EXCLUDED.phone_number_encrypted,
                hire_date = EXCLUDED.hire_date,
                department = EXCLUDED.department,
                job_title = EXCLUDED.job_title,
                salary_encrypted = EXCLUDED.salary_encrypted,
                currency = EXCLUDED.currency,
                employment_status = EXCLUDED.employment_status,
                manager_id = EXCLUDED.manager_id,
                is_active = EXCLUDED.is_active,
                skills = EXCLUDED.skills,
                address = EXCLUDED.address,
                emergency_contact = EXCLUDED.emergency_contact,
                ingestion_timestamp = EXCLUDED.ingestion_timestamp,
                execution_id = EXCLUDED.execution_id,
                source_creation_time = EXCLUDED.source_creation_time
            """;

    private final ObjectMapper mapper;

    EmployeeStatementBinder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    void bind(PreparedStatement statement, EncryptedEmployee row) throws SQLException {
        EmployeeRecord employee = row.employee();
        statement.setString(1, employee.getEmployeeId());
        statement.setString(2, employee.getFirstName());
        statement.setString(3, employee.getLastName());
        statement.setString(4, employee.getEmail());
        statement.setString(5, row.encryptedPhoneNumber());
        statement.setDate(6, Date.valueOf(LocalDate.parse(employee.getHireDate())));
        statement.setString(7, employee.getDepartment());
        statement.setString(8, employee.getJobTitle());
        statement.setString(9, row.encryptedSalary());
        statement.setString(10, employee.getCurrency());
        statement.setString(11, employee.getEmploymentStatus());
        statement.setString(12, employee.getManagerId());
        setNullableBoolean(statement, 13, employee.getIsActive());
        JdbcSupport.setJsonb(statement, 14, employee.getSkills(), mapper);
        JdbcSupport.setJsonb(statement, 15, employee.getAddress(), mapper);
        JdbcSupport.setJsonb(statement, 16, emergencyContactJson(employee.getEmergencyContact(),
                row.encryptedEmergencyContactPhone()), mapper);
        statement.setTimestamp(17, Timestamp.from(row.enriched().ingestionTimestamp()));
        statement.setObject(18, UUID.fromString(row.executionId()));
        statement.setTimestamp(19, Timestamp.from(row.enriched().sourceCreationTime()));
    }

    // Same fields, but phone holds the encrypted value.
    private ObjectNode emergencyContactJson(EmergencyContact contact, String encryptedPhone) {
        if (contact == null) {
            return null;
        }
        ObjectNode json = mapper.createObjectNode();
        json.put("name", contact.getName());
        json.put("relationship", contact.getRelationship());
        json.put("phone", encryptedPhone);
        json.put("email", contact.getEmail());
        return json;
    }

    private static void setNullableBoolean(PreparedStatement statement, int index, Boolean value)
            throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.BOOLEAN);
        } else {
            statement.setBoolean(index, value);
        }
    }
}
