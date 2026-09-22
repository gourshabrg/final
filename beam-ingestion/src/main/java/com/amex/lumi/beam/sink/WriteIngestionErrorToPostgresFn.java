package com.amex.lumi.beam.sink;

import com.amex.lumi.beam.model.EmployeeRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.TimeZone;

/**
 * Persists record-level ingestion failures for audit and troubleshooting.
 */
public class WriteIngestionErrorToPostgresFn
        extends DoFn<LoadFailure, Void> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    WriteIngestionErrorToPostgresFn.class
            );

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient ObjectMapper objectMapper;

    public WriteIngestionErrorToPostgresFn(
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
                INSERT INTO ingestion_error (
                    execution_id,
                    source_file,
                    record_number,
                    error_type,
                    error_message,
                    raw_record
                )
                VALUES (
                    ?,
                    ?,
                    ?,
                    ?,
                    ?,
                    ?::jsonb
                )
                """
        );
    }

        /**
         * Stores one sanitized failure record in PostgreSQL.
         *
         * @param context Beam processing context
         * @throws Exception when the failure record cannot be persisted
         */
    @ProcessElement
    public void processElement(
            ProcessContext context)
            throws Exception {

        LoadFailure failure =
                context.element();

        try {

            setParameters(failure);

            statement.executeUpdate();

            connection.commit();

            LOGGER.warn(
                    "Ingestion error stored in PostgreSQL: recordNumber={}, employeeId={}, executionId={}, errorType={}",
                    failure.getRecordNumber(),
                    failure.getEmployee().getEmployeeId(),
                    failure.getExecutionId(),
                    failure.getErrorType()
            );

        } catch (SQLException exception) {

            rollbackTransaction();

            /*
             * Failure to write the error/audit record is a
             * system-level problem.
             *
             * We must not silently lose the error.
             */
            LOGGER.error(
                    "Unable to store ingestion error in PostgreSQL: recordNumber={}, executionId={}",
                    failure.getRecordNumber(),
                    failure.getExecutionId(),
                    exception
            );

            throw exception;
        }
    }

    private void setParameters(
            LoadFailure failure)
            throws SQLException {

        EmployeeRecord employee =
                failure.getEmployee();

        statement.setObject(
                1,
                java.util.UUID.fromString(
                        failure.getExecutionId()
                )
        );

        statement.setString(
                2,
                failure.getSourceFile()
        );

        statement.setLong(
                3,
                failure.getRecordNumber()
        );

        statement.setString(
                4,
                failure.getErrorType()
        );

        statement.setString(
                5,
                failure.getErrorMessage()
        );

        setRawRecord(
                6,
                employee
        );
    }

    private void setRawRecord(
            int parameter,
            EmployeeRecord employee)
            throws SQLException {

        /*
         * Do NOT store the original EmployeeRecord directly.
         *
         * It contains plaintext:
         *
         * - phone_number
         * - salary
         * - emergency_contact.phone
         *
         * We sanitize those fields before storing the
         * operational error record.
         */
        try {

            SanitizedErrorRecord sanitizedRecord =
                    SanitizedErrorRecord.from(employee);

            PGobject jsonObject =
                    new PGobject();

            jsonObject.setType("jsonb");

            jsonObject.setValue(
                    objectMapper.writeValueAsString(
                            sanitizedRecord
                    )
            );

            statement.setObject(
                    parameter,
                    jsonObject
            );

        } catch (Exception exception) {

            throw new SQLException(
                    "Unable to serialize sanitized error record",
                    exception
            );
        }
    }

    private void rollbackTransaction() {

        if (connection == null) {
            return;
        }

        try {

            connection.rollback();

        } catch (SQLException rollbackException) {

            LOGGER.error(
                    "Unable to rollback ingestion error transaction",
                    rollbackException
            );
        }
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
                    "Unable to close ingestion error statement",
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
                    "Unable to close ingestion error connection",
                    exception
            );
        }
    }
}
