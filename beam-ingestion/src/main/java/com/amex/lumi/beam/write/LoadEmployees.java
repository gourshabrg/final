package com.amex.lumi.beam.write;

import com.amex.lumi.beam.common.IngestionMetrics;
import com.amex.lumi.beam.model.EncryptedEmployee;
import com.amex.lumi.beam.model.RecordFailure;
import com.amex.lumi.beam.model.RecordFailure.FailureType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Saves employees. One commit per row, so a bad row fails alone and the rest still load.
 */
public class LoadEmployees extends PTransform<PCollection<EncryptedEmployee>, PCollectionTuple> {

    public static final TupleTag<EncryptedEmployee> LOADED = new TupleTag<>() {
    };
    public static final TupleTag<RecordFailure> FAILED = new TupleTag<>() {
    };

    private final DatabaseConfig database;

    public LoadEmployees(DatabaseConfig database) {
        this.database = database;
    }

    @Override
    public PCollectionTuple expand(PCollection<EncryptedEmployee> rows) {
        return rows.apply("UpsertEmployeeRows",
                ParDo.of(new WriteEmployeeFn(database)).withOutputTags(LOADED, TupleTagList.of(FAILED)));
    }

    static class WriteEmployeeFn extends DoFn<EncryptedEmployee, EncryptedEmployee> {

        private static final Logger LOGGER = LoggerFactory.getLogger(WriteEmployeeFn.class);
        static final String STALE_MESSAGE = "a newer file already loaded this employee_id; the older record is skipped";

        private final Counter loaded = IngestionMetrics.counter(IngestionMetrics.RECORDS_LOADED);
        private final Counter loadErrors = IngestionMetrics.counter(IngestionMetrics.LOAD_ERRORS);
        private final Counter staleRecords = IngestionMetrics.counter(IngestionMetrics.STALE_RECORDS);
        private final DatabaseConfig database;

        // Opened once in @Setup, reused for every row.
        private transient Connection connection;
        private transient PreparedStatement statement;
        private transient EmployeeStatementBinder binder;

        WriteEmployeeFn(DatabaseConfig database) {
            this.database = database;
        }

        @Setup
        public void setup() throws SQLException {
            connection = database.openConnection();
            statement = connection.prepareStatement(EmployeeStatementBinder.UPSERT_SQL);
            binder = new EmployeeStatementBinder(new ObjectMapper());
            LOGGER.info("Employee writer connected to {}", database.jdbcUrl());
        }

        @ProcessElement
        public void processElement(@Element EncryptedEmployee row, MultiOutputReceiver out) throws SQLException {
            try {
                binder.bind(statement, row);
                int changed = statement.executeUpdate();
                connection.commit();
                if (changed == 0) {
                    rejectStale(row, out);
                    return;
                }
                loaded.inc();
                LOGGER.debug("Loaded employee_id={} (record {})", row.employee().getEmployeeId(), row.recordNumber());
                out.get(LOADED).output(row);
            } catch (SQLException exception) {
                JdbcSupport.rollbackQuietly(connection, LOGGER);
                if (!JdbcSupport.isRecordLevelError(exception)) {
                    // Database problem, not a bad row: stop the job.
                    LOGGER.error("Database error while saving employee_id={}, stopping the job",
                            row.employee().getEmployeeId(), exception);
                    throw exception;
                }
                loadErrors.inc();
                LOGGER.warn("Database rejected employee_id={} (record {}): {}",
                        row.employee().getEmployeeId(), row.recordNumber(), exception.getMessage());
                out.get(FAILED).output(new RecordFailure(row.recordNumber(), row.sourceFile(),
                        row.splitFile(), row.executionId(), FailureType.LOAD_ERROR, exception.getMessage(),
                        row.employee()));
            }
        }

        // The upsert changed nothing because the stored row came from a newer file.
        private void rejectStale(EncryptedEmployee row, MultiOutputReceiver out) {
            staleRecords.inc();
            LOGGER.warn("Skipped employee_id={} (record {}): {}", row.employee().getEmployeeId(), row.recordNumber(),
                    STALE_MESSAGE);
            out.get(FAILED).output(new RecordFailure(row.recordNumber(), row.sourceFile(), row.splitFile(),
                    row.executionId(), FailureType.STALE_ERROR, STALE_MESSAGE, row.employee()));
        }

        @Teardown
        public void teardown() {
            JdbcSupport.closeQuietly(statement, LOGGER);
            JdbcSupport.closeQuietly(connection, LOGGER);
        }
    }
}
