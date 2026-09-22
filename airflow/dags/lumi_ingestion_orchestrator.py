import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.operators.bash import BashOperator

from airflow.utils.trigger_rule import TriggerRule


logger = logging.getLogger(__name__)


DAG_ID = "lumi_ingestion_orchestrator"


with DAG(
    dag_id=DAG_ID,
    description="Orchestrates the Lumi ingestion pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "lumi",
        "ingestion",
        "beam",
        "pyspark",
    ],
) as dag:

    # ==========================================================
    # 1. LOG REQUEST
    # ==========================================================

    def log_ingestion_request(**context):

        dag_run = context["dag_run"]
        conf = dag_run.conf or {}

        execution_id = conf.get("execution_id")
        input_file = conf.get("input_file")
        file_type = conf.get("file_type")
        control_file = conf.get("control_file")

        logger.info(
            "LUMI INGESTION STARTED | "
            "execution_id=%s | "
            "file_type=%s | "
            "input_file=%s | "
            "control_file=%s",
            execution_id,
            file_type,
            input_file,
            control_file,
        )

    log_request = PythonOperator(
        task_id="log_ingestion_request",
        python_callable=log_ingestion_request,
    )

    # ==========================================================
    # 2. VALIDATE REQUEST
    # ==========================================================

    def validate_request(**context):

        dag_run = context["dag_run"]
        conf = dag_run.conf or {}

        required_fields = [
            "execution_id",
            "input_file",
            "file_type",
            "control_file",
            "requires_split",
        ]

        missing_fields = [
            field
            for field in required_fields
            if field not in conf
        ]

        if missing_fields:
            raise ValueError(
                "Missing required DAG configuration fields: "
                + ", ".join(missing_fields)
            )

        logger.info(
            "Request validation successful | execution_id=%s",
            conf["execution_id"],
        )

    validate = PythonOperator(
        task_id="validate_request",
        python_callable=validate_request,
    )

    # ==========================================================
    # 3. CHECK FILE SIZE
    # ==========================================================

    def check_file_size(**context):

        dag_run = context["dag_run"]
        conf = dag_run.conf or {}

        execution_id = conf["execution_id"]
        file_size = conf.get("file_size_bytes")
        threshold = conf.get(
            "file_size_threshold_bytes"
        )

        if file_size is None or threshold is None:
            logger.warning(
                "File size information was not provided | "
                "execution_id=%s",
                execution_id,
            )
            return

        logger.info(
            "File size check | "
            "execution_id=%s | "
            "file_size_bytes=%s | "
            "threshold_bytes=%s | "
            "requires_split=%s",
            execution_id,
            file_size,
            threshold,
            conf["requires_split"],
        )

    file_size_check = PythonOperator(
        task_id="check_file_size",
        python_callable=check_file_size,
    )

    # ==========================================================
    # 4. DECIDE WHETHER SPLIT IS REQUIRED
    # ==========================================================

    def decide_split(**context):

        dag_run = context["dag_run"]
        conf = dag_run.conf or {}

        requires_split = conf.get(
            "requires_split",
            False,
        )

        execution_id = conf["execution_id"]

        if requires_split:
            logger.info(
                "Large file detected | "
                "execution_id=%s | "
                "PySpark split is required",
                execution_id,
            )

            return "run_pyspark_split"

        logger.info(
            "File is within configured threshold | "
            "execution_id=%s | "
            "PySpark split is not required",
            execution_id,
        )

        return "skip_pyspark_split"

    split_decision = BranchPythonOperator(
        task_id="decide_split",
        python_callable=decide_split,
    )

    # ==========================================================
    # 5A. RUN PYSPARK
    # ==========================================================

    run_pyspark = BashOperator(
    task_id="run_pyspark_split",
    bash_command="""
    set -e

    echo "=========================================="
    echo "LUMI PYSPARK FILE SPLIT"
    echo "=========================================="

    EXECUTION_ID="{{ dag_run.conf['execution_id'] }}"
    INPUT_FILE="{{ dag_run.conf['input_file'] }}"
    FILE_TYPE="{{ dag_run.conf['file_type'] }}"

    OUTPUT_DIR="/opt/lumi/data/split/${EXECUTION_ID}"

    echo "Execution ID: ${EXECUTION_ID}"
    echo "Input File: ${INPUT_FILE}"
    echo "File Type: ${FILE_TYPE}"
    echo "Output Directory: ${OUTPUT_DIR}"

    python /opt/lumi/pyspark/jobs/split_file.py \
        --input-file "${INPUT_FILE}" \
        --file-type "${FILE_TYPE}" \
        --execution-id "${EXECUTION_ID}" \
        --output-dir "${OUTPUT_DIR}" \
        --records-per-file 50

    echo "PySpark split completed successfully"
    echo "Output Directory: ${OUTPUT_DIR}"
    """,
)
    skip_pyspark = EmptyOperator(
    task_id="skip_pyspark_split",
)


    # ==========================================================
    # 6. JOIN SPLIT BRANCH
    # ==========================================================

    split_join = EmptyOperator(
        task_id="split_join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ==========================================================
    # 7. RUN APACHE BEAM
    # ==========================================================

    def log_beam_start(**context):

        dag_run = context["dag_run"]
        conf = dag_run.conf or {}

        logger.info(
            "Starting Apache Beam ingestion | "
            "execution_id=%s | "
            "file_type=%s",
            conf["execution_id"],
            conf["file_type"],
        )

    beam_start = PythonOperator(
        task_id="log_beam_start",
        python_callable=log_beam_start,
    )

    run_beam = BashOperator(
    task_id="run_beam_pipeline",
    bash_command="""
        set -e

        echo "=========================================="
        echo "LUMI APACHE BEAM INGESTION"
        echo "=========================================="

        EXECUTION_ID="{{ dag_run.conf['execution_id'] }}"
        ORIGINAL_INPUT_FILE="{{ dag_run.conf['input_file'] }}"
        CONTROL_FILE="{{ dag_run.conf['control_file'] }}"
        FILE_TYPE="{{ dag_run.conf['file_type'] }}"
        REQUIRES_SPLIT="{{ dag_run.conf['requires_split'] | string | lower }}"

        echo "Execution ID: ${EXECUTION_ID}"
        echo "Original Input File: ${ORIGINAL_INPUT_FILE}"
        echo "Control File: ${CONTROL_FILE}"
        echo "File Type: ${FILE_TYPE}"
        echo "Requires Split: ${REQUIRES_SPLIT}"

        #
        # Select the input that Beam should process.
        #
        # Small file:
        #   Beam reads the original input file.
        #
        # Large file:
        #   PySpark has created split JSON files under:
        #   /opt/lumi/data/split/<execution-id>/
        #
          if [ "${REQUIRES_SPLIT}" = "true" ]; then

    echo "Phase 2 large-file processing selected"

    if [ "${FILE_TYPE}" = "CSV" ]; then

        INPUT_FILE="/opt/lumi/data/split/${EXECUTION_ID}/*.csv"
        SPLIT_EXTENSION="csv"

    elif [ "${FILE_TYPE}" = "JSON" ]; then

        INPUT_FILE="/opt/lumi/data/split/${EXECUTION_ID}/*.json"
        SPLIT_EXTENSION="json"

    else

        echo "ERROR: Unsupported file type: ${FILE_TYPE}"
        exit 1

    fi

    echo "File type: ${FILE_TYPE}"
    echo "Beam input pattern: ${INPUT_FILE}"

    SPLIT_DIRECTORY="/opt/lumi/data/split/${EXECUTION_ID}"

    if [ ! -d "${SPLIT_DIRECTORY}" ]; then
        echo "ERROR: PySpark split directory does not exist: ${SPLIT_DIRECTORY}"
        exit 1
    fi

    SPLIT_FILE_COUNT=$(find "${SPLIT_DIRECTORY}" \
        -maxdepth 1 \
        -type f \
        -name "*.${SPLIT_EXTENSION}" | wc -l)

    echo "Expected split extension: .${SPLIT_EXTENSION}"
    echo "Number of split files: ${SPLIT_FILE_COUNT}"

    if [ "${SPLIT_FILE_COUNT}" -eq 0 ]; then
        echo "ERROR: No .${SPLIT_EXTENSION} split files found in ${SPLIT_DIRECTORY}"
        exit 1
    fi

else

    INPUT_FILE="${ORIGINAL_INPUT_FILE}"

    echo "Phase 1 small-file processing selected"
    echo "Beam input file: ${INPUT_FILE}"

fi


        ERROR_OUTPUT="/opt/lumi/data/error/execution-${EXECUTION_ID}"

        #
        # Source creation time must always come from the
        # original source file, not from a split file.
        #
        SOURCE_CREATION_TIME=$(date -u -d "@$(stat -c %Y "${ORIGINAL_INPUT_FILE}")" +"%Y-%m-%dT%H:%M:%SZ")

        INGESTION_TIMESTAMP="{{ ts }}"

        echo "Ingestion timestamp: ${INGESTION_TIMESTAMP}"
        echo "Source creation time: ${SOURCE_CREATION_TIME}"
        echo "Error output: ${ERROR_OUTPUT}"

        echo "Starting Java Apache Beam pipeline..."

        java -jar /opt/lumi/beam/beam-ingestion-1.0.0-SNAPSHOT.jar \
            --executionId="${EXECUTION_ID}" \
            --inputFile="${INPUT_FILE}" \
            --fileType="${FILE_TYPE}" \
            --ingestionTimestamp="${INGESTION_TIMESTAMP}" \
            --sourceCreationTime="${SOURCE_CREATION_TIME}" \
            --errorOutput="${ERROR_OUTPUT}" \
            --encryptionKey="${LUMI_ENCRYPTION_KEY}" \
            --jdbcUrl="jdbc:postgresql://postgres:5432/warehouse" \
            --jdbcUsername="airflow" \
            --jdbcPassword="airflow" \
            --controlFile="${CONTROL_FILE}"

        echo "Apache Beam pipeline completed"
        echo "=========================================="
    """,
)



    # ==========================================================
    # 8. CHECK EXECUTION STATUS
    # ==========================================================

    def check_execution_status(**context):

        dag_run = context["dag_run"]
        conf = dag_run.conf or {}

        execution_id = conf["execution_id"]

        logger.info(
            "Checking ingestion execution status | "
            "execution_id=%s",
            execution_id,
        )

        #
        # Beam persists the final status in the
        # ingestion_execution table.
        #
        # A dedicated DB/API status check can be added here.
        #

        logger.info(
            "Beam task completed for execution_id=%s",
            execution_id,
        )

    execution_status = PythonOperator(
        task_id="check_execution_status",
        python_callable=check_execution_status,
    )

    # ==========================================================
    # 9. FINALIZE
    # ==========================================================

    def finalize_ingestion(**context):

        dag_run = context["dag_run"]
        conf = dag_run.conf or {}

        logger.info(
            "LUMI INGESTION WORKFLOW COMPLETED | "
            "execution_id=%s",
            conf["execution_id"],
        )

    finalize = PythonOperator(
        task_id="finalize_ingestion",
        python_callable=finalize_ingestion,
    )

    # ==========================================================
    # DEPENDENCIES
    # ==========================================================

log_request >> validate >> file_size_check >> split_decision

split_decision >> run_pyspark
split_decision >> skip_pyspark

run_pyspark >> split_join
skip_pyspark >> split_join

split_join >> beam_start >> run_beam

run_beam >> execution_status >> finalize
