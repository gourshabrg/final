"""
Tests for the DAG and its task functions. Run inside the scheduler container:
    docker exec lumi-airflow-scheduler python -m unittest discover -s /opt/airflow/tests -v
"""
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, "/opt/airflow/dags")

from airflow.exceptions import AirflowException  # noqa: E402
from airflow.models import DagBag  # noqa: E402

from lumi_ingestion import tasks  # noqa: E402

DAG_ID = "lumi_ingestion_orchestrator"


def context_for(conf, xcom=None):
    task_instance = SimpleNamespace(xcom_pull=lambda task_ids: xcom)
    return {"dag_run": SimpleNamespace(conf=conf), "ti": task_instance}


class DagStructureTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dag_bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)

    def test_dag_loads_without_errors(self):
        self.assertEqual({}, self.dag_bag.import_errors)
        self.assertIn(DAG_ID, self.dag_bag.dags)

    def test_dag_has_all_tasks(self):
        dag = self.dag_bag.dags[DAG_ID]
        self.assertEqual(
            {"log_request", "validate_request", "choose_split", "run_pyspark_split", "skip_pyspark_split",
             "split_join", "run_beam_pipeline", "check_execution_status", "finalize"},
            set(dag.task_ids))

    def test_tasks_run_in_the_right_order(self):
        dag = self.dag_bag.dags[DAG_ID]
        self.assertEqual({"run_pyspark_split", "skip_pyspark_split"},
                         dag.get_task("choose_split").downstream_task_ids)
        self.assertEqual({"run_beam_pipeline"}, dag.get_task("split_join").downstream_task_ids)
        self.assertEqual({"finalize"}, dag.get_task("check_execution_status").downstream_task_ids)

    def test_join_runs_after_a_skipped_branch(self):
        dag = self.dag_bag.dags[DAG_ID]
        self.assertEqual("none_failed_min_one_success", dag.get_task("split_join").trigger_rule)

    def test_safe_tasks_retry_and_final_steps_do_not(self):
        dag = self.dag_bag.dags[DAG_ID]
        self.assertEqual(2, dag.get_task("validate_request").retries)
        self.assertEqual(2, dag.get_task("run_pyspark_split").retries)
        self.assertEqual(0, dag.get_task("run_beam_pipeline").retries)
        self.assertEqual(0, dag.get_task("check_execution_status").retries)

    def test_dag_only_runs_when_triggered(self):
        self.assertIsNone(self.dag_bag.dags[DAG_ID].schedule_interval)


class TaskFunctionTest(unittest.TestCase):

    def setUp(self):
        folder = tempfile.mkdtemp()
        self.input_file = os.path.join(folder, "employees.csv")
        self.control_file = os.path.join(folder, "employees.properties")
        for path in (self.input_file, self.control_file):
            with open(path, "w") as file:
                file.write("x")
        self.conf = {
            "execution_id": "abc", "input_file": self.input_file, "file_type": "CSV",
            "control_file": self.control_file, "requires_split": False,
            "split_output_dir": "/tmp/split", "error_output": "/tmp/error",
            "file_size_bytes": 10, "file_size_threshold_bytes": 100,
        }

    def test_valid_request_passes(self):
        tasks.validate_request(**context_for(self.conf))

    def test_missing_parameter_fails(self):
        del self.conf["control_file"]
        with self.assertRaisesRegex(AirflowException, "control_file"):
            tasks.validate_request(**context_for(self.conf))

    def test_unsupported_file_type_fails(self):
        self.conf["file_type"] = "XML"
        with self.assertRaisesRegex(AirflowException, "Unsupported file_type"):
            tasks.validate_request(**context_for(self.conf))

    def test_file_not_visible_in_container_fails(self):
        self.conf["input_file"] = "/nope/employees.csv"
        with self.assertRaisesRegex(AirflowException, "not found inside Airflow"):
            tasks.validate_request(**context_for(self.conf))

    def test_small_file_skips_split(self):
        self.assertEqual("skip_pyspark_split", tasks.choose_split_branch(**context_for(self.conf)))

    def test_large_file_is_split(self):
        self.conf["requires_split"] = True
        self.assertEqual("run_pyspark_split", tasks.choose_split_branch(**context_for(self.conf)))

    def test_successful_run_returns_counts(self):
        with mock.patch.object(tasks, "read_execution_summary", return_value=("SUCCESS", 20, 20, None, 0)):
            result = tasks.check_execution_status(**context_for(self.conf))
        self.assertEqual({"expected": 20, "loaded": 20, "error_records": 0}, result)

    def test_failed_run_fails_the_task(self):
        summary = ("FAILED", 20, 16, "Record count mismatch", 4)
        with mock.patch.object(tasks, "read_execution_summary", return_value=summary):
            with self.assertRaisesRegex(AirflowException, "Record count mismatch"):
                tasks.check_execution_status(**context_for(self.conf))

    def test_unknown_run_fails_the_task(self):
        with mock.patch.object(tasks, "read_execution_summary", return_value=None):
            with self.assertRaisesRegex(AirflowException, "No ingestion_execution row"):
                tasks.check_execution_status(**context_for(self.conf))

    def test_finalize_reads_counts_from_xcom(self):
        with self.assertLogs("lumi.ingestion", level="INFO") as logs:
            tasks.finalize(**context_for(self.conf, xcom={"loaded": 20, "error_records": 0}))
        self.assertIn("loaded=20", logs.output[0])


if __name__ == "__main__":
    unittest.main()
