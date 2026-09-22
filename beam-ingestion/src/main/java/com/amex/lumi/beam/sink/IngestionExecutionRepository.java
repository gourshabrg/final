package com.amex.lumi.beam.sink;

import com.amex.lumi.beam.model.IngestionExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

/**
 * Persists ingestion lifecycle state in PostgreSQL.
 *
 * <p>Each save operation uses a local transaction so a failed update is
 * rolled back before the original database exception is propagated.</p>
 */
public class IngestionExecutionRepository {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(IngestionExecutionRepository.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public IngestionExecutionRepository(
            String jdbcUrl,
            String username,
            String password) {

        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

        /**
         * Inserts or updates the lifecycle state for one ingestion execution.
         *
         * @param execution lifecycle state to persist
         * @throws SQLException when the database operation cannot be completed
         */
        public void save(IngestionExecution execution)
                        throws SQLException {

                Properties properties = new Properties();
                properties.setProperty("user", username);
                properties.setProperty("password", password);
                properties.setProperty("options", "-c TimeZone=UTC");

    try (

         Connection connection =
                 DriverManager.getConnection(jdbcUrl, properties);

         PreparedStatement statement =
                 connection.prepareStatement(
                         """
                         INSERT INTO ingestion_execution (
                             execution_id,
                             source_file,
                             status,
                             expected_record_count,
                             actual_loaded_record_count,
                             started_at,
                             completed_at,
                             failure_reason
                         )
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                         ON CONFLICT (execution_id)
                         DO UPDATE SET
                             source_file = EXCLUDED.source_file,
                             status = EXCLUDED.status,
                             expected_record_count =
                                 EXCLUDED.expected_record_count,
                             actual_loaded_record_count =
                                 EXCLUDED.actual_loaded_record_count,
                             started_at = EXCLUDED.started_at,
                             completed_at = EXCLUDED.completed_at,
                             failure_reason =
                                 EXCLUDED.failure_reason
                         """
                 )) {

        connection.setAutoCommit(false);

        try {
            setParameters(statement, execution);

            statement.executeUpdate();

            connection.commit();

        } catch (SQLException exception) {

            connection.rollback();

            LOGGER.error(
                    "Unable to persist ingestion execution: executionId={}, status={}",
                    execution.getExecutionId(),
                    execution.getStatus(),
                    exception
            );

            throw exception;
        }
    }
        }


    private void setParameters(
            PreparedStatement statement,
            IngestionExecution execution)
            throws SQLException {

        statement.setObject(
                1,
                UUID.fromString(
                        execution.getExecutionId()
                )
        );

        statement.setString(
                2,
                execution.getSourceFile()
        );

        statement.setString(
                3,
                execution.getStatus().name()
        );

        setNullableLong(
                statement,
                4,
                execution.getExpectedRecordCount()
        );

        setNullableLong(
                statement,
                5,
                execution.getActualLoadedRecordCount()
        );

        statement.setTimestamp(
                6,
                toSqlTimestamp(
                        execution.getStartedAt()
                )
        );

        if (execution.getCompletedAt() == null) {
            statement.setNull(
                    7,
                    Types.TIMESTAMP_WITH_TIMEZONE
            );
        } else {
            statement.setTimestamp(
                    7,
                    toSqlTimestamp(
                            execution.getCompletedAt()
                    )
            );
        }

        statement.setString(
                8,
                execution.getFailureReason()
        );
    }

    private void setNullableLong(
            PreparedStatement statement,
            int parameter,
            Long value)
            throws SQLException {

        if (value == null) {
            statement.setNull(
                    parameter,
                    Types.BIGINT
            );
            return;
        }

        statement.setLong(parameter, value);
    }

    private java.sql.Timestamp toSqlTimestamp(
            Instant instant) {

        return instant == null
                ? null
                : java.sql.Timestamp.from(instant);
    }
}
