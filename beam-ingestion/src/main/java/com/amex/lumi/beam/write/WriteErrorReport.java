package com.amex.lumi.beam.write;

import com.amex.lumi.beam.model.RecordFailure;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;

/**
 * Writes failed records to the error text file and the ingestion_error table.
 */
public class WriteErrorReport extends PTransform<PCollection<RecordFailure>, PDone> {

    private final String errorFilePrefix;
    private final DatabaseConfig database;

    public WriteErrorReport(String errorFilePrefix, DatabaseConfig database) {
        this.errorFilePrefix = errorFilePrefix;
        this.database = database;
    }

    @Override
    public PDone expand(PCollection<RecordFailure> failures) {
        failures.apply("StoreFailuresInDatabase", ParDo.of(new InsertFailureFn(database)));

        return failures
                .apply("FormatFailureLines", MapElements.into(TypeDescriptors.strings())
                        .via(ErrorLineFormatter::format))
                // One file instead of many part files.
                .apply("WriteErrorFile", TextIO.write().to(errorFilePrefix).withSuffix(".txt").withoutSharding());
    }

    static class InsertFailureFn extends DoFn<RecordFailure, Void> {

        private static final Logger LOGGER = LoggerFactory.getLogger(InsertFailureFn.class);

        private static final String INSERT_SQL = """
                INSERT INTO ingestion_error
                    (execution_id, source_file, record_number, error_type, error_message, raw_record)
                VALUES (?, ?, ?, ?, ?, ?)
                """;

        private final DatabaseConfig database;
        private transient Connection connection;
        private transient PreparedStatement statement;
        private transient ObjectMapper mapper;

        InsertFailureFn(DatabaseConfig database) {
            this.database = database;
        }

        @Setup
        public void setup() throws SQLException {
            mapper = new ObjectMapper();
            connection = database.openConnection();
            statement = connection.prepareStatement(INSERT_SQL);
            LOGGER.debug("Error writer connected to {}", database.jdbcUrl());
        }

        @ProcessElement
        public void processElement(@Element RecordFailure failure) throws SQLException {
            try {
                statement.setObject(1, UUID.fromString(failure.executionId()));
                statement.setString(2, failure.sourceFile());
                statement.setLong(3, failure.recordNumber());
                statement.setString(4, failure.type().name());
                statement.setString(5, failure.message());
                if (failure.employee() == null) {
                    statement.setNull(6, Types.OTHER);
                } else {
                    JdbcSupport.setJsonb(statement, 6, RedactedEmployeeJson.from(failure.employee(), mapper), mapper);
                }
                statement.executeUpdate();
                connection.commit();
            } catch (SQLException exception) {
                JdbcSupport.rollbackQuietly(connection, LOGGER);
                // Never lose an error record silently.
                LOGGER.error("Could not store failure for record {} of {}", failure.recordNumber(),
                        failure.sourceFile(), exception);
                throw exception;
            }
        }

        @Teardown
        public void teardown() {
            JdbcSupport.closeQuietly(statement, LOGGER);
            JdbcSupport.closeQuietly(connection, LOGGER);
        }
    }
}
