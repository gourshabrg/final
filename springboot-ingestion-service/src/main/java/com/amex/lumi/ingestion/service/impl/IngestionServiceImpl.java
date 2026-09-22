package com.amex.lumi.ingestion.service.impl;

import com.amex.lumi.ingestion.client.AirflowClient;
import com.amex.lumi.ingestion.dto.AirflowDagRunRequest;
import com.amex.lumi.ingestion.dto.IngestionRequest;
import com.amex.lumi.ingestion.dto.IngestionResponse;
import com.amex.lumi.ingestion.service.AirflowPathMapper;
import com.amex.lumi.ingestion.service.FileValidator;
import com.amex.lumi.ingestion.service.IngestionService;
import com.amex.lumi.ingestion.config.IngestionProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class IngestionServiceImpl implements IngestionService {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(IngestionServiceImpl.class);

    private final FileValidator fileValidator;
    private final AirflowClient airflowClient;
    private final AirflowPathMapper airflowPathMapper;
    private final IngestionProperties ingestionProperties;
    
    public IngestionServiceImpl(
            FileValidator fileValidator,
            AirflowClient airflowClient,
            AirflowPathMapper airflowPathMapper,
            IngestionProperties ingestionProperties) {

        this.fileValidator = fileValidator;
        this.airflowClient = airflowClient;
        this.airflowPathMapper = airflowPathMapper;
        this.ingestionProperties = ingestionProperties;
    }

    @Override
    public IngestionResponse startIngestion(
            IngestionRequest request) {

        Path filePath =
                Path.of(request.getFileLocation());

        /*
         * 1. Validate the source file.
         */
        fileValidator.validate(
                filePath,
                request.getFileType()
        );

        /*
         * 2. Validate control file.
         */
        Path controlFilePath =
                Path.of(request.getControlFileLocation());

        validateControlFile(controlFilePath);

        /*
         * 3. Generate unique execution ID.
         */
        UUID executionId =
                UUID.randomUUID();

        /*
         * 4. Determine source file size.
         */
        long fileSizeBytes =
                getFileSize(filePath);

        /*
         * 5. Determine whether PySpark splitting
         *    is required.
         */
        long threshold =
                ingestionProperties
                        .getFileSizeThresholdBytes();

        boolean requiresSplit =
                fileSizeBytes > threshold;

        LOGGER.info(
                "Ingestion request validated: executionId={}, fileType={}, fileSizeBytes={}, requiresSplit={}",
                executionId,
                request.getFileType(),
                fileSizeBytes,
                requiresSplit
        );

      /*
 * 6. Convert source file path
 *    to Airflow container path.
 */
String airflowFilePath =
        airflowPathMapper.toAirflowPath(
                request.getFileLocation()
        );


/*
 * 7. Convert control file path
 *    to Airflow container path.
 */
String airflowControlFilePath =
        airflowPathMapper.toAirflowControlFilePath(
                request.getControlFileLocation()
        );


        /*
         * 8. Build Airflow DAG configuration.
         */
        Map<String, Object> dagConf =
                new HashMap<>();

        dagConf.put(
                "execution_id",
                executionId.toString()
        );

        dagConf.put(
                "input_file",
                airflowFilePath
        );

        dagConf.put(
                "file_type",
                request.getFileType().name()
        );

        dagConf.put(
                "control_file",
                airflowControlFilePath
        );

        dagConf.put(
                "file_size_bytes",
                fileSizeBytes
        );

        dagConf.put(
                "file_size_threshold_bytes",
                threshold
        );

        dagConf.put(
                "requires_split",
                requiresSplit
        );

        /*
         * 9. Airflow DAG run ID.
         */
        String dagRunId =
                "ingestion_" + executionId;

        AirflowDagRunRequest airflowRequest =
                new AirflowDagRunRequest(
                        dagRunId,
                        dagConf
                );

        /*
         * 10. Trigger Airflow.
         */
        airflowClient.triggerDag(
                airflowRequest
        );

        LOGGER.info(
                "Ingestion submitted successfully: executionId={}, fileType={}, requiresSplit={}",
                executionId,
                request.getFileType(),
                requiresSplit
        );

        /*
         * 11. Return immediately.
         *
         * Airflow performs the actual ingestion asynchronously.
         */
        return new IngestionResponse(
                executionId,
                "ACCEPTED",
                "Ingestion request accepted and Airflow DAG triggered"
        );
    }

    private void validateControlFile(
            Path controlFilePath) {

        if (!Files.exists(controlFilePath)) {
            throw new IllegalArgumentException(
                    "Control file does not exist: "
                            + controlFilePath
            );
        }

        if (!Files.isRegularFile(controlFilePath)) {
            throw new IllegalArgumentException(
                    "Control file is not a regular file: "
                            + controlFilePath
            );
        }

        if (!Files.isReadable(controlFilePath)) {
            throw new IllegalArgumentException(
                    "Control file is not readable: "
                            + controlFilePath
            );
        }
    }

    private long getFileSize(Path filePath) {

        try {
            return Files.size(filePath);
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Unable to determine input file size: "
                            + filePath,
                    exception
            );
        }
    }
}
