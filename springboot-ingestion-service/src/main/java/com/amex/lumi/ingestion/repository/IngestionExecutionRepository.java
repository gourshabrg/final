package com.amex.lumi.ingestion.repository;

import com.amex.lumi.ingestion.dto.IngestionStatusResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Reads run status written by the Beam job.
 */
@Repository
public class IngestionExecutionRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionExecutionRepository.class);

    private static final String FIND_BY_ID = """
            SELECT e.execution_id, e.source_file, e.status, e.expected_record_count,
                   e.actual_loaded_record_count, e.started_at, e.completed_at, e.failure_reason,
                   (SELECT COUNT(*) FROM ingestion_error err WHERE err.execution_id = e.execution_id) AS error_count
              FROM ingestion_execution e
             WHERE e.execution_id = ?
            """;

    private final JdbcClient jdbcClient;

    public IngestionExecutionRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    public Optional<IngestionStatusResponse> findById(UUID executionId) {
        LOGGER.debug("Loading status of execution {}", executionId);
        return jdbcClient.sql(FIND_BY_ID)
                .param(executionId)
                .query((row, rowNumber) -> map(row))
                .optional();
    }

    private static IngestionStatusResponse map(ResultSet row) throws SQLException {
        return new IngestionStatusResponse(
                row.getObject("execution_id", UUID.class),
                row.getString("source_file"),
                row.getString("status"),
                row.getObject("expected_record_count", Long.class),
                row.getObject("actual_loaded_record_count", Long.class),
                row.getLong("error_count"),
                toInstant(row.getTimestamp("started_at")),
                toInstant(row.getTimestamp("completed_at")),
                row.getString("failure_reason"));
    }

    private static Instant toInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }
}
