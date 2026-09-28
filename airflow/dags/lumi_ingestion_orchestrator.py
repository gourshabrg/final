"""
Lumi ingestion DAG, triggered by the Spring Boot API with the run details in dag_run.conf.

log_request -> validate_request -> choose_split -> run_pyspark_split / skip_pyspark_split
            -> split_join -> run_beam_pipeline -> check_execution_status -> finalize
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from lumi_ingestion import tasks

SCRIPTS_DIR = "/opt/airflow/scripts"

# Values the bash scripts read; Airflow fills in the {{ }} templates per run.
RUN_ENV = {
    "EXECUTION_ID": "{{ dag_run.conf['execution_id'] }}",
    "INPUT_FILE": "{{ dag_run.conf['input_file'] }}",
    "FILE_TYPE": "{{ dag_run.conf['file_type'] }}",
    "CONTROL_FILE": "{{ dag_run.conf['control_file'] }}",
    "REQUIRES_SPLIT": "{{ dag_run.conf['requires_split'] | string | lower }}",
    "SPLIT_OUTPUT_DIR": "{{ dag_run.conf['split_output_dir'] }}",
    "ERROR_OUTPUT": "{{ dag_run.conf['error_output'] }}",
    # What the API read at request time; Beam fails the run if the control file changed since.
    "EXPECTED_RECORD_COUNT": "{{ dag_run.conf.get('expected_record_count', '') }}",
}

# Retry short hiccups (e.g. a busy scheduler) instead of failing the whole ingestion.
DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(seconds=30)}

with DAG(
    dag_id="lumi_ingestion_orchestrator",
    default_args=DEFAULT_ARGS,
    description="Ingests CSV/JSON employee files into the warehouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Only runs when the API triggers it.
    catchup=False,
    max_active_runs=4,
    # Marks the run FAILED in the database even when Beam was killed and could not do it itself.
    on_failure_callback=tasks.on_dag_failure,
    tags=["lumi", "ingestion", "beam", "pyspark"],
) as dag:

    log_request = PythonOperator(task_id="log_request", python_callable=tasks.log_request)

    validate_request = PythonOperator(task_id="validate_request", python_callable=tasks.validate_request)

    choose_split = BranchPythonOperator(task_id="choose_split", python_callable=tasks.choose_split_branch)

    # The trailing space stops Airflow from treating the .sh path as a Jinja template file.
    run_pyspark_split = BashOperator(
        task_id="run_pyspark_split",
        bash_command=f"bash {SCRIPTS_DIR}/run_pyspark_split.sh ",
        env=RUN_ENV,
        append_env=True,
        # A stuck Spark job is killed instead of holding a worker slot forever.
        execution_timeout=timedelta(minutes=30),
    )

    skip_pyspark_split = EmptyOperator(task_id="skip_pyspark_split")

    # Runs after whichever branch was taken (the other one is skipped).
    split_join = EmptyOperator(task_id="split_join", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    # No retry: a record-count mismatch would only fail again after reloading everything.
    run_beam_pipeline = BashOperator(
        task_id="run_beam_pipeline",
        bash_command=f"bash {SCRIPTS_DIR}/run_beam_pipeline.sh ",
        env=RUN_ENV,
        append_env=True,
        retries=0,
        # A stuck Beam job is killed; on_dag_failure then marks the run FAILED.
        execution_timeout=timedelta(hours=1),
    )

    # No retry; ALL_DONE so it also logs the saved reason after Beam fails.
    check_execution_status = PythonOperator(
        task_id="check_execution_status", python_callable=tasks.check_execution_status, retries=0,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    finalize = PythonOperator(task_id="finalize", python_callable=tasks.finalize)

    log_request >> validate_request >> choose_split >> [run_pyspark_split, skip_pyspark_split] >> split_join
    split_join >> run_beam_pipeline >> check_execution_status >> finalize
