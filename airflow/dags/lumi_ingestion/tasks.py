"""Python functions run by the lumi_ingestion_orchestrator DAG tasks."""
import logging
import os

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger("lumi.ingestion")

WAREHOUSE_CONN_ID = "lumi_warehouse"
SUPPORTED_FILE_TYPES = ("CSV", "JSON")
SUCCESS = "SUCCESS"
REQUIRED_CONF_KEYS = (
    "execution_id",
    "input_file",
    "file_type",
    "control_file",
    "requires_split",
    "split_output_dir",
    "error_output",
)


def _conf(context):
    return context["dag_run"].conf or {}


def _log(level, step, conf, message, *args):
    # Same "[step] execution_id=..." start on every line, so one run can be found with a single search.
    logger.log(level, "[%s] execution_id=%s | " + message, step, conf.get("execution_id"), *args)


def _fail(step, conf, message):
    # Log as ERROR, then fail the task so Airflow shows it red.
    _log(logging.ERROR, step, conf, message)
    raise AirflowException(message)


def log_request(**context):
    conf = _conf(context)
    _log(logging.INFO, "request", conf, "file=%s type=%s control_file=%s size=%s bytes",
         conf.get("input_file"), conf.get("file_type"), conf.get("control_file"), conf.get("file_size_bytes"))


def validate_request(**context):
    conf = _conf(context)
    missing = [key for key in REQUIRED_CONF_KEYS if key not in conf]
    if missing:
        _fail("validate", conf, "Missing DAG parameters: " + ", ".join(missing))

    if conf["file_type"] not in SUPPORTED_FILE_TYPES:
        _fail("validate", conf, f"Unsupported file_type {conf['file_type']}; expected CSV or JSON")

    # The API checked the files on the host; here we check the container can see them too.
    for key in ("input_file", "control_file"):
        if not os.path.isfile(conf[key]):
            _fail("validate", conf, f"{key} not found inside Airflow: {conf[key]}")

    _log(logging.INFO, "validate", conf, "parameters and files are valid")


def choose_split_branch(**context):
    conf = _conf(context)
    size = conf.get("file_size_bytes")
    threshold = conf.get("file_size_threshold_bytes")
    if conf["requires_split"]:
        _log(logging.INFO, "split-decision", conf, "size %s > threshold %s bytes -> split with PySpark",
             size, threshold)
        return "run_pyspark_split"
    _log(logging.INFO, "split-decision", conf, "size %s <= threshold %s bytes -> no split", size, threshold)
    return "skip_pyspark_split"


def read_execution_summary(execution_id):
    """Returns (status, expected, loaded, failure_reason, error_count) or None when the run is unknown."""
    hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
    row = hook.get_first(
        """
        SELECT status, expected_record_count, actual_loaded_record_count, failure_reason,
               (SELECT COUNT(*) FROM ingestion_error WHERE execution_id = %s)
          FROM ingestion_execution
         WHERE execution_id = %s
        """,
        parameters=(execution_id, execution_id),
    )
    return None if row is None else tuple(row)


def check_execution_status(**context):
    conf = _conf(context)
    summary = read_execution_summary(conf["execution_id"])
    if summary is None:
        _fail("status", conf, "No ingestion_execution row found for this run")

    status, expected, loaded, reason, error_count = summary
    _log(logging.INFO, "status", conf, "status=%s expected=%s loaded=%s error_records=%s",
         status, expected, loaded, error_count)
    if status != SUCCESS:
        _fail("status", conf, f"Ingestion ended with status {status}: {reason}")
    if error_count:
        _log(logging.WARNING, "status", conf, "%s record(s) were rejected, see %s.txt",
             error_count, conf.get("error_output"))

    # The return value is saved in XCom so the next task can read it.
    return {"expected": expected, "loaded": loaded, "error_records": error_count}


def finalize(**context):
    conf = _conf(context)
    summary = context["ti"].xcom_pull(task_ids="check_execution_status") or {}
    _log(logging.INFO, "finalize", conf, "ingestion completed | loaded=%s error_records=%s error_file=%s.txt",
         summary.get("loaded"), summary.get("error_records"), conf.get("error_output"))
