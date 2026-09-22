package com.amex.lumi.beam.sink;

import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.model.EncryptedEmployeeRecord;
import com.amex.lumi.beam.model.EmergencyContact;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.TimeZone;

public class WriteEmployeeToPostgresFn
        extends DoFn<
        EncryptedEmployeeRecord,
        EncryptedEmployeeRecord> {


    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    WriteEmployeeToPostgresFn.class
            );

    public static final TupleTag<LoadFailure> LOAD_FAILURES =
            new TupleTag<>() {};
            
     public static final TupleTag<EncryptedEmployeeRecord> LOADED_RECORDS =
        new TupleTag<>() {};
       

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient ObjectMapper objectMapper;

    public WriteEmployeeToPostgresFn(
            String jdbcUrl,
            String username,
            String password) {

        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Setup
    public void setup() throws SQLException {

        objectMapper = new ObjectMapper();

        /*
         * PostgreSQL was previously receiving the legacy
         * Asia/Calcutta timezone from the JVM.
         *
         * UTC is used because the ingestion pipeline
         * works with explicit ISO-8601 timestamps.
         */
        TimeZone.setDefault(
                TimeZone.getTimeZone("UTC")
        );

        connection = DriverManager.getConnection(
                jdbcUrl,
                username,
                password
        );

        connection.setAutoCommit(false);

        statement = connection.prepareStatement(
                """
                INSERT INTO employee (
                    employee_id,
                    first_name,
                    last_name,
                    email,
                    phone_number_encrypted,
                    hire_date,
                    department,
                    job_title,
                    salary_encrypted,
                    currency,
                    employment_status,
                    manager_id,
                    is_active,
                    skills,
                    address,
                    emergency_contact,
                    ingestion_timestamp,
                    execution_id,
                    source_creation_time
                )
                VALUES (
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?::jsonb,
                    ?::jsonb,
                    ?::jsonb,
                    ?,
                    ?,
                    ?
                )
                ON CONFLICT (employee_id)
                DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    phone_number_encrypted =
                        EXCLUDED.phone_number_encrypted,
                    hire_date = EXCLUDED.hire_date,
                    department = EXCLUDED.department,
                    job_title = EXCLUDED.job_title,
                    salary_encrypted =
                        EXCLUDED.salary_encrypted,
                    currency = EXCLUDED.currency,
                    employment_status =
                        EXCLUDED.employment_status,
                    manager_id = EXCLUDED.manager_id,
                    is_active = EXCLUDED.is_active,
                    skills = EXCLUDED.skills,
                    address = EXCLUDED.address,
                    emergency_contact =
                        EXCLUDED.emergency_contact,
                    ingestion_timestamp =
                        EXCLUDED.ingestion_timestamp,
                    execution_id =
                        EXCLUDED.execution_id,
                    source_creation_time =
                        EXCLUDED.source_creation_time
                """
        );
    }

    @ProcessElement
    public void processElement(
            ProcessContext context) throws Exception {

        EncryptedEmployeeRecord record =
                context.element();

        EmployeeRecord employee =
                record.getEmployee();

        try {

            setParameters(record);

            statement.executeUpdate();

connection.commit();

LOGGER.info(
        "Employee record loaded into PostgreSQL: employeeId={}, recordNumber={}, executionId={}",
        employee.getEmployeeId(),
        record.getRecordNumber(),
        record.getExecutionId()
);

context.output(record);


        } catch (SQLException | JsonProcessingException exception) {

            /*
             * Roll back the current record transaction.
             *
             * This prevents a failed SQL operation from
             * leaving the JDBC transaction in an aborted state.
             */
            rollbackTransaction();

            if (isRecordLevelFailure(exception)) {

                LoadFailure failure =
                        new LoadFailure(
                                employee,
                                record.getRecordNumber(),
                                record.getSourceFile(),
                                "LOAD_ERROR",
                                sanitizeErrorMessage(
                                        exception.getMessage()
                                ),
                                record.getExecutionId()
                        );

                LOGGER.warn(
                        "Employee record failed to load: employeeId={}, recordNumber={}, executionId={}, errorType=LOAD_ERROR",
                        employee.getEmployeeId(),
                        record.getRecordNumber(),
                        record.getExecutionId()
                );

                context.output(
                        LOAD_FAILURES,
                        failure
                );

                return;
            }

            /*
             * Database connectivity/authentication/system
             * failures must fail the pipeline.
             */
            throw exception;
        }
    }

    private boolean isRecordLevelFailure(
            Exception exception) {

        if (!(exception instanceof SQLException sqlException)) {
            return false;
        }

        /*
         * SQLState class 23 = Integrity Constraint Violation.
         *
         * Examples:
         * 23505 = unique violation
         * 23502 = not-null violation
         * 23503 = foreign-key violation
         * 22001 = string data right truncation
         *
         * These are generally record/data problems.
         */
        String sqlState =
                sqlException.getSQLState();

        if (sqlState == null) {
            return false;
        }

        return sqlState.startsWith("23")
                || sqlState.startsWith("22");
    }

    private void rollbackTransaction() {

        if (connection == null) {
            return;
        }

        try {

            connection.rollback();

        } catch (SQLException rollbackException) {

            LOGGER.error(
                    "Unable to rollback PostgreSQL transaction",
                    rollbackException
            );
        }
    }

    private String sanitizeErrorMessage(
            String errorMessage) {

        if (errorMessage == null) {
            return "Unknown database error";
        }

        return errorMessage
                .replace("|", " ")
                .replace("\r", " ")
                .replace("\n", " ");
    }

    private void setParameters(
            EncryptedEmployeeRecord record)
            throws SQLException, JsonProcessingException {

        EmployeeRecord employee =
                record.getEmployee();

        int parameter = 1;

        statement.setString(
                parameter++,
                employee.getEmployeeId()
        );

        statement.setString(
                parameter++,
                employee.getFirstName()
        );

        statement.setString(
                parameter++,
                employee.getLastName()
        );

        statement.setString(
                parameter++,
                employee.getEmail()
        );

        statement.setString(
                parameter++,
                record.getEncryptedPhoneNumber()
        );

        setHireDate(
                parameter++,
                employee.getHireDate()
        );

        statement.setString(
                parameter++,
                employee.getDepartment()
        );

        statement.setString(
                parameter++,
                employee.getJobTitle()
        );

        statement.setString(
                parameter++,
                record.getEncryptedSalary()
        );

        statement.setString(
                parameter++,
                employee.getCurrency()
        );

        statement.setString(
                parameter++,
                employee.getEmploymentStatus()
        );

        statement.setString(
                parameter++,
                employee.getManagerId()
        );

        if (employee.getIsActive() == null) {

            statement.setNull(
                    parameter++,
                    java.sql.Types.BOOLEAN
            );

        } else {

            statement.setBoolean(
                    parameter++,
                    employee.getIsActive()
            );
        }

        setJson(
                parameter++,
                employee.getSkills()
        );

        setJson(
                parameter++,
                employee.getAddress()
        );

        setEmergencyContact(
                parameter++,
                employee.getEmergencyContact(),
                record.getEncryptedEmergencyContactPhone()
        );

        statement.setTimestamp(
                parameter++,
                java.sql.Timestamp.from(
                        record.getIngestionTimestamp()
                )
        );

        statement.setObject(
                parameter++,
                java.util.UUID.fromString(
                        record.getExecutionId()
                )
        );

        statement.setTimestamp(
                parameter,
                java.sql.Timestamp.from(
                        record.getSourceCreationTime()
                )
        );
    }

    private void setHireDate(
            int parameter,
            String hireDate)
            throws SQLException {

        if (hireDate == null
                || hireDate.isBlank()) {

            statement.setNull(
                    parameter,
                    java.sql.Types.DATE
            );

            return;
        }

        LocalDate date =
                LocalDate.parse(hireDate);

        statement.setDate(
                parameter,
                Date.valueOf(date)
        );
    }

    private void setJson(
            int parameter,
            Object value)
            throws SQLException, JsonProcessingException {

        if (value == null) {

            statement.setNull(
                    parameter,
                    java.sql.Types.OTHER
            );

            return;
        }

        PGobject jsonObject =
                new PGobject();

        jsonObject.setType("jsonb");

        jsonObject.setValue(
                objectMapper.writeValueAsString(value)
        );

        statement.setObject(
                parameter,
                jsonObject
        );
    }

    private void setEmergencyContact(
            int parameter,
            EmergencyContact contact,
            String encryptedPhone)
            throws SQLException, JsonProcessingException {

        if (contact == null) {

            statement.setNull(
                    parameter,
                    java.sql.Types.OTHER
            );

            return;
        }

        EmergencyContactDatabaseValue databaseValue =
                new EmergencyContactDatabaseValue(
                        contact.getName(),
                        contact.getRelationship(),
                        encryptedPhone,
                        contact.getEmail()
                );

        PGobject jsonObject =
                new PGobject();

        jsonObject.setType("jsonb");

        jsonObject.setValue(
                objectMapper.writeValueAsString(
                        databaseValue
                )
        );

        statement.setObject(
                parameter,
                jsonObject
        );
    }

    @Teardown
    public void teardown() {

        closeStatement();

        closeConnection();
    }

    private void closeStatement() {

        if (statement == null) {
            return;
        }

        try {

            statement.close();

        } catch (SQLException exception) {

            LOGGER.warn(
                    "Unable to close PostgreSQL prepared statement",
                    exception
            );
        }
    }

    private void closeConnection() {

        if (connection == null) {
            return;
        }

        try {

            connection.close();

        } catch (SQLException exception) {

            LOGGER.warn(
                    "Unable to close PostgreSQL connection",
                    exception
            );
        }
    }

    private static class EmergencyContactDatabaseValue {

        private final String name;
        private final String relationship;
        private final String phone;
        private final String email;

        public EmergencyContactDatabaseValue(
                String name,
                String relationship,
                String phone,
                String email) {

            this.name = name;
            this.relationship = relationship;
            this.phone = phone;
            this.email = email;
        }

        public String getName() {
            return name;
        }

        public String getRelationship() {
            return relationship;
        }

        public String getPhone() {
            return phone;
        }

        public String getEmail() {
            return email;
        }
    }

}
