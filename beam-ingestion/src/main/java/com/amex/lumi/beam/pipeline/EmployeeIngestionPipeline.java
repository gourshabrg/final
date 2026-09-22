package com.amex.lumi.beam.pipeline;

import com.amex.lumi.beam.common.IngestionMetrics;
import com.amex.lumi.beam.common.PipelineConstants;
import com.amex.lumi.beam.execution.ControlFileReader;
import com.amex.lumi.beam.execution.ExecutionStatusRepository;
import com.amex.lumi.beam.execution.RecordCountCheck;
import com.amex.lumi.beam.model.IngestionControl;
import com.amex.lumi.beam.model.IngestionExecution;
import com.amex.lumi.beam.options.IngestionPipelineOptions;
import com.amex.lumi.beam.write.DatabaseConfig;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.util.TimeZone;

/**
 * Start of the Beam job. Airflow runs it with: java -jar beam-ingestion.jar --inputFile=... --fileType=...
 */
public final class EmployeeIngestionPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmployeeIngestionPipeline.class);

    private EmployeeIngestionPipeline() {
    }

    public static void main(String[] args) throws SQLException {
        TimeZone.setDefault(TimeZone.getTimeZone(PipelineConstants.TIME_ZONE));

        IngestionPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(IngestionPipelineOptions.class);
        String executionId = options.getExecutionId();
        Instant startedAt = Instant.now();
        LOGGER.info("Starting ingestion executionId={} fileType={} input={}",
                executionId, options.getFileType(), options.getInputFile());

        DatabaseConfig database = new DatabaseConfig(
                options.getJdbcUrl(), options.getJdbcUsername(), options.getJdbcPassword());
        ExecutionStatusRepository statusRepository = new ExecutionStatusRepository(database);

        IngestionControl control = readControlFile(options, startedAt, statusRepository);
        RecordCountCheck.RunContext run = new RecordCountCheck.RunContext(
                executionId, options.getInputFile(), control.expectedRecordCount(), startedAt);

        statusRepository.save(IngestionExecution.started(
                executionId, run.sourceFile(), run.expectedRecordCount(), startedAt));

        Pipeline pipeline = Pipeline.create(options);
        IngestionGraph.build(pipeline, options, database, run);

        statusRepository.save(IngestionExecution.running(
                executionId, run.sourceFile(), run.expectedRecordCount(), startedAt));
        runAndWait(pipeline, executionId, statusRepository);
    }

    // A bad control file stops the run before any data is read, but the FAILED status is still saved.
    private static IngestionControl readControlFile(IngestionPipelineOptions options, Instant startedAt,
                                                    ExecutionStatusRepository statusRepository) throws SQLException {
        try {
            IngestionControl control = new ControlFileReader().read(options.getControlFile());
            LOGGER.info("Control file expects {} record(s)", control.expectedRecordCount());
            return control;
        } catch (IllegalArgumentException exception) {
            LOGGER.error("Control file problem, run stopped: {}", exception.getMessage());
            statusRepository.save(IngestionExecution.failed(options.getExecutionId(), options.getInputFile(),
                    null, null, startedAt, Instant.now(), exception.getMessage()));
            throw exception;
        }
    }

    private static void runAndWait(Pipeline pipeline, String executionId,
                                   ExecutionStatusRepository statusRepository) throws SQLException {
        try {
            PipelineResult result = pipeline.run();
            PipelineResult.State state = result.waitUntilFinish();
            IngestionMetrics.logSummary(result, executionId, LOGGER);
            if (state != PipelineResult.State.DONE) {
                throw new IllegalStateException("Pipeline finished in state " + state);
            }
            LOGGER.info("Ingestion finished successfully executionId={}", executionId);
        } catch (RuntimeException exception) {
            // Any crash: mark the run FAILED so it never stays stuck at RUNNING.
            String reason = rootMessage(exception);
            LOGGER.error("Ingestion failed executionId={}: {}", executionId, reason);
            statusRepository.markFailedIfUnfinished(executionId, reason);
            throw exception;
        }
    }

    private static String rootMessage(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage();
    }
}
