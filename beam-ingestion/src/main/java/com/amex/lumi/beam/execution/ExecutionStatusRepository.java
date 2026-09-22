package com.amex.lumi.beam.execution;

import com.amex.lumi.beam.model.IngestionExecution;
import com.amex.lumi.beam.write.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.UUID;

/**
 * Saves run status in ingestion_execution (one row per run).
 */
public class ExecutionStatusRepository implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionStatusRepository.class);

    private static final String UPSERT_SQL = """
            INSERT INTO ingestion_execution (
                execution_id, source_file, status, expected_record_count,
                actual_loaded_record_count, started_at, completed_at, failure_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (execution_id) DO UPDATE SET
                source_file = EXCLUDED.source_file,
                status = EXCLUDED.status,
                expected_record_count = EXCLUDED.expected_record_count,
                actual_loaded_record_count = EXCLUDED.actual_loaded_record_count,
                started_at = EXCLUDED.started_at,
                completed_at = EXCLUDED.completed_at,
                failure_reason = EXCLUDED.failure_reason
            """;

    // Only changes runs that are not finished yet, so an existing FAILED reason is kept.
    private static final String MARK_FAILED_SQL = """
            UPDATE ingestion_execution
               SET status = 'FAILED', completed_at = ?, failure_reason = ?
             WHERE execution_id = ? AND status IN ('STARTED', 'RUNNING')
            """;

    private final DatabaseConfig database;

    public ExecutionStatusRepository(DatabaseConfig database) {
        this.database = database;
    }

    public void save(IngestionExecution execution) throws SQLException {
        try (Connection connection = database.openConnection();
             PreparedStatement statement = connection.prepareStatement(UPSERT_SQL)) {
            statement.setObject(1, UUID.fromString(execution.executionId()));
            statement.setString(2, execution.sourceFile());
            statement.setString(3, execution.status().name());
            setNullableLong(statement, 4, execution.expectedRecordCount());
            setNullableLong(statement, 5, execution.actualLoadedRecordCount());
            statement.setTimestamp(6, toTimestamp(execution.startedAt()));
            statement.setTimestamp(7, toTimestamp(execution.completedAt()));
            statement.setString(8, execution.failureReason());
            executeAndCommit(connection, statement);
        } catch (SQLException exception) {
            LOGGER.error("Could not save status {} for execution {}", execution.status(), execution.executionId(),
                    exception);
            throw exception;
        }
        LOGGER.info("Execution {} is now {}", execution.executionId(), execution.status());
    }

    public void markFailedIfUnfinished(String executionId, String reason) throws SQLException {
        try (Connection connection = database.openConnection();
             PreparedStatement statement = connection.prepareStatement(MARK_FAILED_SQL)) {
            statement.setTimestamp(1, Timestamp.from(Instant.now()));
            statement.setString(2, reason);
            statement.setObject(3, UUID.fromString(executionId));
            if (executeAndCommit(connection, statement) > 0) {
                LOGGER.warn("Execution {} marked FAILED: {}", executionId, reason);
            }
        } catch (SQLException exception) {
            LOGGER.error("Could not mark execution {} as FAILED", executionId, exception);
            throw exception;
        }
    }

    private static int executeAndCommit(Connection connection, PreparedStatement statement) throws SQLException {
        try {
            int rows = statement.executeUpdate();
            connection.commit();
            return rows;
        } catch (SQLException exception) {
            connection.rollback();
            throw exception;
        }
    }

    private static void setNullableLong(PreparedStatement statement, int index, Long value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.BIGINT);
        } else {
            statement.setLong(index, value);
        }
    }

    private static Timestamp toTimestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }
}
