package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.client.AirflowDagRunRequest;
import com.amex.lumi.ingestion.model.FileType;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static com.amex.lumi.ingestion.client.DagRunConfKeys.CONTROL_FILE;
import static com.amex.lumi.ingestion.client.DagRunConfKeys.ERROR_OUTPUT;
import static com.amex.lumi.ingestion.client.DagRunConfKeys.EXECUTION_ID;
import static com.amex.lumi.ingestion.client.DagRunConfKeys.EXPECTED_RECORD_COUNT;
import static com.amex.lumi.ingestion.client.DagRunConfKeys.FILE_SIZE_BYTES;
import static com.amex.lumi.ingestion.client.DagRunConfKeys.FILE_SIZE_THRESHOLD_BYTES;
import static com.amex.lumi.ingestion.client.DagRunConfKeys.FILE_TYPE;
import static com.amex.lumi.ingestion.client.DagRunConfKeys.INPUT_FILE;
import static com.amex.lumi.ingestion.client.DagRunConfKeys.REQUIRES_SPLIT;
import static com.amex.lumi.ingestion.client.DagRunConfKeys.SPLIT_OUTPUT_DIR;

/**
 * Builds the DAG parameters (dag_run.conf). Every path is the path inside the Airflow container.
 */
@Component
public class DagRunRequestFactory {

    private static final String DAG_RUN_ID_PREFIX = "ingestion_";

    private final IngestionPathResolver paths;

    public DagRunRequestFactory(IngestionPathResolver paths) {
        this.paths = paths;
    }

    /** Everything the DAG needs to know about one run. */
    public record RunDetails(UUID executionId, Path dataFile, Path controlFile, FileType fileType,
                             long fileSizeBytes, long thresholdBytes, long expectedRecordCount,
                             boolean requiresSplit) {
    }

    public AirflowDagRunRequest create(RunDetails run) {
        String id = run.executionId().toString();

        Map<String, Object> conf = new LinkedHashMap<>();
        conf.put(EXECUTION_ID, id);
        conf.put(INPUT_FILE, paths.toAirflowDataPath(run.dataFile()));
        conf.put(FILE_TYPE, run.fileType().name());
        conf.put(CONTROL_FILE, paths.toAirflowControlPath(run.controlFile()));
        conf.put(EXPECTED_RECORD_COUNT, run.expectedRecordCount());
        conf.put(FILE_SIZE_BYTES, run.fileSizeBytes());
        conf.put(FILE_SIZE_THRESHOLD_BYTES, run.thresholdBytes());
        conf.put(REQUIRES_SPLIT, run.requiresSplit());
        // Phase 2: the folder where PySpark writes the split files that Beam then reads.
        conf.put(SPLIT_OUTPUT_DIR, paths.airflowSplitDirectory(id));
        conf.put(ERROR_OUTPUT, paths.airflowErrorOutput(id));

        return new AirflowDagRunRequest(dagRunId(run.executionId()), conf);
    }

    public static String dagRunId(UUID executionId) {
        return DAG_RUN_ID_PREFIX + executionId;
    }
}
