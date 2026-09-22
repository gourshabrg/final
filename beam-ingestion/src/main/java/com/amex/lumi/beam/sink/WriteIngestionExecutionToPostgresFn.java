package com.amex.lumi.beam.sink;

import com.amex.lumi.beam.model.IngestionExecution;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * Writes ingestion lifecycle updates through a worker-local repository.
 */
public class WriteIngestionExecutionToPostgresFn
        extends DoFn<IngestionExecution, Void> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    WriteIngestionExecutionToPostgresFn.class
            );

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private transient IngestionExecutionRepository repository;

    public WriteIngestionExecutionToPostgresFn(
            String jdbcUrl,
            String username,
            String password) {

        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Setup
    public void setup() {

        repository =
                new IngestionExecutionRepository(
                        jdbcUrl,
                        username,
                        password
                );
    }

        /**
         * Persists one lifecycle update and emits no downstream element.
         *
         * @param context Beam processing context
         * @throws SQLException when the lifecycle update cannot be persisted
         */
    @ProcessElement
    public void processElement(
            ProcessContext context)
            throws SQLException {

        IngestionExecution execution =
                context.element();

        repository.save(execution);

        LOGGER.info(
                "Ingestion execution status stored: executionId={}, status={}",
                execution.getExecutionId(),
                execution.getStatus()
        );
    }
}
