package com.amex.lumi.ingestion.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

/**
 * Reads employees from the warehouse.
 */
@Repository
public class EmployeeRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmployeeRepository.class);

    private static final String FIND_BY_ID = """
            SELECT employee_id, first_name, last_name, email, phone_number_encrypted, hire_date,
                   department, job_title, salary_encrypted, currency, employment_status, manager_id,
                   is_active, skills::text AS skills, address::text AS address,
                   emergency_contact::text AS emergency_contact,
                   ingestion_timestamp, execution_id, source_creation_time
              FROM employee
             WHERE employee_id = ?
            """;

    private final JdbcClient jdbcClient;

    public EmployeeRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    public Optional<EmployeeRow> findById(String employeeId) {
        LOGGER.debug("Loading employee {}", employeeId);
        return jdbcClient.sql(FIND_BY_ID)
                .param(employeeId)
                .query((row, rowNumber) -> map(row))
                .optional();
    }

    private static EmployeeRow map(ResultSet row) throws SQLException {
        Date hireDate = row.getDate("hire_date");
        return new EmployeeRow(
                row.getString("employee_id"),
                row.getString("first_name"),
                row.getString("last_name"),
                row.getString("email"),
                row.getString("phone_number_encrypted"),
                hireDate == null ? null : hireDate.toLocalDate(),
                row.getString("department"),
                row.getString("job_title"),
                row.getString("salary_encrypted"),
                row.getString("currency"),
                row.getString("employment_status"),
                row.getString("manager_id"),
                row.getObject("is_active", Boolean.class),
                row.getString("skills"),
                row.getString("address"),
                row.getString("emergency_contact"),
                row.getTimestamp("ingestion_timestamp").toInstant(),
                row.getObject("execution_id", UUID.class),
                row.getTimestamp("source_creation_time").toInstant());
    }
}
