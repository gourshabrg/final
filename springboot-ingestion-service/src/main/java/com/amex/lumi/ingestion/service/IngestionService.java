package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.client.AirflowClient;
import com.amex.lumi.ingestion.client.AirflowDagRunRequest;
import com.amex.lumi.ingestion.common.LogKeys;
import com.amex.lumi.ingestion.config.IngestionProperties;
import com.amex.lumi.ingestion.dto.IngestionRequest;
import com.amex.lumi.ingestion.dto.IngestionResponse;
import com.amex.lumi.ingestion.dto.IngestionStatusResponse;
import com.amex.lumi.ingestion.exception.ResourceNotFoundException;
import com.amex.lumi.ingestion.repository.IngestionExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.util.UUID;

/**
 * Checks an ingestion request, creates the execution ID and starts the Airflow DAG.
 */
@Service
public class IngestionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestionService.class);
    private static final String ACCEPTED = "ACCEPTED";

    private final IngestionPathResolver paths;
    private final SourceFileValidator sourceFileValidator;
    private final ControlFileValidator controlFileValidator;
    private final DagRunRequestFactory dagRunRequestFactory;
    private final AirflowClient airflowClient;
    private final IngestionExecutionRepository executionRepository;
    private final long thresholdBytes;

    public IngestionService(IngestionPathResolver paths,
                            SourceFileValidator sourceFileValidator,
                            ControlFileValidator controlFileValidator,
                            DagRunRequestFactory dagRunRequestFactory,
                            AirflowClient airflowClient,
                            IngestionExecutionRepository executionRepository,
                            IngestionProperties properties) {
        this.paths = paths;
        this.sourceFileValidator = sourceFileValidator;
        this.controlFileValidator = controlFileValidator;
        this.dagRunRequestFactory = dagRunRequestFactory;
        this.airflowClient = airflowClient;
        this.executionRepository = executionRepository;
        this.thresholdBytes = properties.fileSizeThresholdBytes();
    }

    public IngestionResponse startIngestion(IngestionRequest request) {
        Path dataFile = paths.resolveDataFile(request.fileLocation());
        Path controlFile = paths.resolveControlFile(request.controlFileLocation());
        long fileSize = sourceFileValidator.validate(dataFile, request.fileType());
        long expectedRecords = controlFileValidator.validate(controlFile);

        UUID executionId = UUID.randomUUID();
        // Phase 2: big files are split by PySpark first.
        boolean requiresSplit = fileSize > thresholdBytes;

        // From here on every log line of this request shows the executionId.
        MDC.put(LogKeys.EXECUTION_ID, executionId.toString());
        try {
            AirflowDagRunRequest dagRun = dagRunRequestFactory.create(new DagRunRequestFactory.RunDetails(
                    executionId, dataFile, controlFile, request.fileType(), fileSize, thresholdBytes,
                    expectedRecords, requiresSplit));

            LOGGER.info("Ingestion accepted: file={} type={} size={} bytes threshold={} bytes split={} expectedRecords={}",
                    dataFile, request.fileType(), fileSize, thresholdBytes, requiresSplit, expectedRecords);
            airflowClient.triggerDag(dagRun);

            return new IngestionResponse(executionId, dagRun.dagRunId(), requiresSplit, ACCEPTED,
                    "Ingestion started in Airflow. Check GET /api/v1/ingestions/" + executionId + " for the result.");
        } finally {
            MDC.remove(LogKeys.EXECUTION_ID);
        }
    }

    public IngestionStatusResponse getStatus(UUID executionId) {
        IngestionStatusResponse status = executionRepository.findById(executionId)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "No ingestion found for executionId " + executionId
                                + " (it may still be waiting in Airflow)"));
        LOGGER.info("Status of {} is {}", executionId, status.status());
        return status;
    }
}
