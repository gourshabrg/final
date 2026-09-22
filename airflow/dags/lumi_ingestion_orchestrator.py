"""
Lumi ingestion DAG, triggered by the Spring Boot API with the run details in dag_run.conf.

log_request -> validate_request -> choose_split -> run_pyspark_split / skip_pyspark_split
            -> split_join -> run_beam_pipeline -> check_execution_status -> finalize
"""
from datetime import datetime

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
}

with DAG(
    dag_id="lumi_ingestion_orchestrator",
    description="Ingests CSV/JSON employee files into the warehouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Only runs when the API triggers it.
    catchup=False,
    max_active_runs=4,
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
    )

    skip_pyspark_split = EmptyOperator(task_id="skip_pyspark_split")

    # Runs after whichever branch was taken (the other one is skipped).
    split_join = EmptyOperator(task_id="split_join", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    run_beam_pipeline = BashOperator(
        task_id="run_beam_pipeline",
        bash_command=f"bash {SCRIPTS_DIR}/run_beam_pipeline.sh ",
        env=RUN_ENV,
        append_env=True,
    )

    check_execution_status = PythonOperator(
        task_id="check_execution_status", python_callable=tasks.check_execution_status
    )

    finalize = PythonOperator(task_id="finalize", python_callable=tasks.finalize)

    log_request >> validate_request >> choose_split >> [run_pyspark_split, skip_pyspark_split] >> split_join
    split_join >> run_beam_pipeline >> check_execution_status >> finalize
