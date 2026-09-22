package com.amex.lumi.ingestion.client;

/**
 * Parameter names sent to the Airflow DAG. They must match the names the DAG reads from dag_run.conf.
 */
public final class DagRunConfKeys {

    public static final String EXECUTION_ID = "execution_id";
    public static final String INPUT_FILE = "input_file";
    public static final String FILE_TYPE = "file_type";
    public static final String CONTROL_FILE = "control_file";
    public static final String EXPECTED_RECORD_COUNT = "expected_record_count";
    public static final String FILE_SIZE_BYTES = "file_size_bytes";
    public static final String FILE_SIZE_THRESHOLD_BYTES = "file_size_threshold_bytes";
    public static final String REQUIRES_SPLIT = "requires_split";
    public static final String SPLIT_OUTPUT_DIR = "split_output_dir";
    public static final String ERROR_OUTPUT = "error_output";

    private DagRunConfKeys() {
    }
}
