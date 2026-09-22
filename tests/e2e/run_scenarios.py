"""
End-to-end scenarios: API -> Airflow DAG -> PySpark split -> Beam -> PostgreSQL.

Needs the running platform (docker compose + API). Only uses the Python standard library.

    python tests/e2e/run_scenarios.py              # checks whatever split mode the API is using
    python tests/e2e/run_scenarios.py --expect split
    python tests/e2e/run_scenarios.py --expect no-split

The API decides split / no split from lumi.ingestion.file-size-threshold-bytes:
threshold 1 byte = every file is split, a large threshold (e.g. 10000000) = no file is split.
"""
import argparse
import base64
import json
import math
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
API = "http://localhost:8080"
AIRFLOW = "http://localhost:8082"
AIRFLOW_AUTH = "Basic " + base64.b64encode(b"airflow:airflow").decode()
DAG_ID = "lumi_ingestion_orchestrator"
CONTAINER_DATA = "/opt/lumi/data"
CONTAINER_CONTROL = "/opt/lumi/control-files"
TIMEOUT_SECONDS = 2400
RETRIES = 5

# name, data file, control file, type, expected status, expected loaded, expected error rows, records in file
API_SCENARIOS = [
    ("csv_small", "samples/employees.csv", "employees_csv.properties", "CSV", "SUCCESS", 20, 0, 20),
    ("json_small", "samples/employees.json", "employees_json.properties", "JSON", "SUCCESS", 20, 0, 20),
    ("csv_with_errors", "samples/employees_with_errors.csv", "employees_with_errors.properties", "CSV",
     "FAILED", 16, 4, 20),
    ("csv_large", "samples/employees_large.csv", "employees_large_csv.properties", "CSV", "SUCCESS", 120, 0, 120),
    ("json_large", "samples/employees_large.json", "employees_large_json.properties", "JSON",
     "SUCCESS", 120, 0, 120),
    # Same file again: must succeed because rows are upserted, not duplicated.
    ("csv_rerun", "samples/employees.csv", "employees_csv.properties", "CSV", "SUCCESS", 20, 0, 20),
]

# Employee to decrypt after a scenario: scenario -> (employee_id, expected phone)
DECRYPT_CHECKS = {"csv_large": ("LGC0001", "9876500001"), "json_large": ("LGJ0001", "9876500001")}

BAD_CONTROL_FILE = "_e2e_bad_record_count.properties"

results = []


def check(scenario, name, passed, detail=""):
    results.append((scenario, name, passed, detail))


def http(method, url, body=None, headers=None):
    data = None if body is None else json.dumps(body).encode()
    for attempt in range(1, RETRIES + 1):
        request = urllib.request.Request(url, data=data, method=method, headers=headers or {})
        request.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.status, json.loads(response.read() or b"null")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"null")
        except (TimeoutError, urllib.error.URLError) as error:
            # Airflow's web server gets slow while Spark and Beam use all the CPU; wait and try again.
            print(f"  {method} {url} did not answer ({error}), retry {attempt}/{RETRIES}", flush=True)
            time.sleep(10)
    raise RuntimeError(f"{method} {url} did not answer after {RETRIES} tries")


def airflow(method, path, body=None):
    return http(method, AIRFLOW + "/api/v1" + path, body, {"Authorization": AIRFLOW_AUTH})


def start_via_api(data_file, control_file, file_type):
    status, body = http("POST", API + "/api/v1/ingestions",
                        {"fileLocation": data_file, "controlFileLocation": control_file, "fileType": file_type})
    if status != 202:
        raise RuntimeError(f"API returned {status}: {body}")
    return body["executionId"], body["requiresSplit"]


def start_via_airflow(conf):
    """Triggers the DAG directly, to test checks the API would normally stop earlier."""
    execution_id = conf["execution_id"]
    status, body = airflow("POST", f"/dags/{DAG_ID}/dagRuns",
                           {"dag_run_id": f"ingestion_{execution_id}", "conf": conf})
    if status != 200:
        raise RuntimeError(f"Airflow returned {status}: {body}")
    return execution_id


def direct_conf(data_file, control_file, file_type):
    execution_id = str(uuid.uuid4())
    return {
        "execution_id": execution_id,
        "input_file": f"{CONTAINER_DATA}/{data_file}",
        "file_type": file_type,
        "control_file": f"{CONTAINER_CONTROL}/{control_file}",
        "requires_split": False,
        "split_output_dir": f"{CONTAINER_DATA}/split/{execution_id}",
        "error_output": f"{CONTAINER_DATA}/error/execution-{execution_id}",
    }


def wait_for(execution_ids):
    """Waits until every DAG run is finished. Returns {execution_id: dag state}."""
    states = {}
    deadline = time.time() + TIMEOUT_SECONDS
    while time.time() < deadline and len(states) < len(execution_ids):
        for execution_id in execution_ids:
            if execution_id in states:
                continue
            _, run = airflow("GET", f"/dags/{DAG_ID}/dagRuns/ingestion_{execution_id}")
            if run and run.get("state") in ("success", "failed"):
                states[execution_id] = run["state"]
        print(f"  {len(states)}/{len(execution_ids)} DAG runs finished", flush=True)
        if len(states) < len(execution_ids):
            time.sleep(20)
    return states


def task_states(execution_id):
    _, body = airflow("GET", f"/dags/{DAG_ID}/dagRuns/ingestion_{execution_id}/taskInstances")
    return {task["task_id"]: task["state"] for task in body["task_instances"]}


def split_file_count(execution_id, file_type):
    folder = ROOT / "data" / "split" / execution_id
    return len(list(folder.glob("*." + file_type.lower()))) if folder.exists() else 0


def verify_api_scenario(scenario, execution_id, requires_split, dag_state, records_per_file):
    name, _, _, file_type, want_status, want_loaded, want_errors, records = scenario
    tasks = task_states(execution_id)
    _, status = http("GET", f"{API}/api/v1/ingestions/{execution_id}")

    check(name, "DAG state", dag_state == ("success" if want_status == "SUCCESS" else "failed"), dag_state)
    check(name, "run status", status.get("status") == want_status, status.get("status"))
    check(name, "loaded rows", status.get("actualLoadedRecordCount") == want_loaded,
          str(status.get("actualLoadedRecordCount")))
    check(name, "error rows", status.get("errorRecordCount") == want_errors, str(status.get("errorRecordCount")))

    if requires_split:
        want_files = math.ceil(records / records_per_file)
        check(name, "split branch ran", tasks.get("run_pyspark_split") == "success"
              and tasks.get("skip_pyspark_split") == "skipped", str(tasks.get("run_pyspark_split")))
        files = split_file_count(execution_id, file_type)
        check(name, f"split files = {want_files}", files == want_files, str(files))
    else:
        check(name, "no-split branch ran", tasks.get("skip_pyspark_split") == "success"
              and tasks.get("run_pyspark_split") == "skipped", str(tasks.get("skip_pyspark_split")))
        check(name, "no split files", split_file_count(execution_id, file_type) == 0)

    # A FAILED run stops at the Beam task, so the later status task is marked upstream_failed.
    want_status_task = "success" if want_status == "SUCCESS" else "upstream_failed"
    check(name, "status task", tasks.get("check_execution_status") == want_status_task,
          str(tasks.get("check_execution_status")))

    error_file = ROOT / "data" / "error" / f"execution-{execution_id}.txt"
    lines = error_file.read_text(encoding="utf-8").splitlines() if error_file.exists() else []
    check(name, "error file lines", len(lines) == want_errors, str(len(lines)))

    if name in DECRYPT_CHECKS:
        employee_id, phone = DECRYPT_CHECKS[name]
        code, employee = http("GET", f"{API}/api/v1/employees/{employee_id}")
        check(name, "decrypted phone", code == 200 and employee.get("phone_number") == phone,
              str(employee.get("phone_number") if code == 200 else code))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--expect", choices=["split", "no-split"], help="fail if the API uses the other mode")
    parser.add_argument("--records-per-file", type=int, default=50, help="LUMI_RECORDS_PER_SPLIT_FILE in Airflow")
    args = parser.parse_args()

    bad_control = ROOT / "control-files" / BAD_CONTROL_FILE
    bad_control.write_text("record_count=twenty\n", encoding="utf-8")
    try:
        print("Starting scenarios through the API ...")
        started = []
        for scenario in API_SCENARIOS:
            execution_id, requires_split = start_via_api(scenario[1], scenario[2], scenario[3])
            started.append((scenario, execution_id, requires_split))
            print(f"  {scenario[0]:<16} {execution_id} split={requires_split}")

        split_modes = {requires_split for _, _, requires_split in started}
        if args.expect:
            check("setup", f"API mode is {args.expect}", split_modes == {args.expect == "split"}, str(split_modes))

        print("Starting DAG safety-net scenarios directly in Airflow ...")
        bad_count_id = start_via_airflow(direct_conf("samples/employees.csv", BAD_CONTROL_FILE, "CSV"))
        missing_file_id = start_via_airflow(direct_conf("samples/does_not_exist.csv", "employees_csv.properties",
                                                        "CSV"))

        all_ids = [execution_id for _, execution_id, _ in started] + [bad_count_id, missing_file_id]
        print("Waiting for the DAG runs ...")
        states = wait_for(all_ids)

        for scenario, execution_id, requires_split in started:
            verify_api_scenario(scenario, execution_id, requires_split, states.get(execution_id),
                                args.records_per_file)

        # Broken control file: Beam must stop before loading and still save FAILED with the reason.
        tasks = task_states(bad_count_id)
        _, status = http("GET", f"{API}/api/v1/ingestions/{bad_count_id}")
        check("bad_control_file", "DAG failed", states.get(bad_count_id) == "failed")
        check("bad_control_file", "beam task failed", tasks.get("run_beam_pipeline") == "failed")
        check("bad_control_file", "status FAILED with reason", status.get("status") == "FAILED"
              and "whole number" in (status.get("failureReason") or ""), str(status.get("failureReason")))

        # File missing inside the container: validate_request must stop the run before Beam.
        tasks = task_states(missing_file_id)
        check("missing_input_file", "DAG failed", states.get(missing_file_id) == "failed")
        check("missing_input_file", "validate task failed", tasks.get("validate_request") == "failed")
        check("missing_input_file", "beam never ran", tasks.get("run_beam_pipeline") == "upstream_failed",
              str(tasks.get("run_beam_pipeline")))
    finally:
        bad_control.unlink(missing_ok=True)

    print()
    print(f"{'SCENARIO':<20}{'CHECK':<28}{'RESULT':<8}DETAIL")
    for scenario, name, passed, detail in results:
        print(f"{scenario:<20}{name:<28}{'PASS' if passed else 'FAIL':<8}{detail}")
    failures = [result for result in results if not result[2]]
    print(f"\n{len(results) - len(failures)} passed, {len(failures)} failed")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
