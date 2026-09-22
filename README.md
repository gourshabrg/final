# Amex Lumi Ingestion

Loads employee **CSV** and **JSON** files into a PostgreSQL warehouse.

```
POST /api/v1/ingestions (Spring Boot)
   -> Airflow DAG  -> PySpark split (large files)  -> Apache Beam job -> PostgreSQL
```

| Module | What it does |
|---|---|
| `springboot-ingestion-service` | REST API: validates the request, creates the execution ID, triggers Airflow, decrypts data |
| `airflow` | DAG that runs each step and logs it |
| `pyspark` | Splits files bigger than the threshold into smaller files |
| `beam-ingestion` | Parses, validates, fills missing values, adds metadata, encrypts, loads, checks record count |
| `database/init` | Warehouse tables, created on first start |
| `data/samples`, `control-files` | Sample input files and their control files |

A step-by-step explanation is in [docs/INTERVIEW_GUIDE.md](docs/INTERVIEW_GUIDE.md).

## Prerequisites

- Docker Desktop (or Docker Engine with the Compose plugin)
- Java 17 or newer
- No Maven install needed: both Java modules include the Maven Wrapper

## 1. Configure

```bash
cp .env.example .env            # Windows PowerShell: Copy-Item .env.example .env
```

Set `LUMI_ENCRYPTION_KEY` in `.env` to exactly 32 characters.
If another PostgreSQL already uses port 5432, uncomment `POSTGRES_HOST_PORT` and `LUMI_WAREHOUSE_JDBC_URL`.

## 2. Build the Beam JAR and start Docker

macOS / Linux:
```bash
(cd beam-ingestion && ./mvnw clean package)
docker compose up -d --build
```

Windows PowerShell:
```powershell
Set-Location beam-ingestion; .\mvnw.cmd clean package; Set-Location ..
docker compose up -d --build
```

Airflow: http://localhost:8082 (user `airflow`, password `airflow`).

## 3. Start the API

Run it from the module folder so the default paths `../data` and `../control-files` are correct.

```bash
cd springboot-ingestion-service
./mvnw spring-boot:run          # Windows: .\mvnw.cmd spring-boot:run
```

Health: http://localhost:8080/actuator/health

## 4. Run an ingestion

`fileLocation` is relative to `data/`, `controlFileLocation` is relative to `control-files/` (absolute paths inside those folders also work).

```bash
curl -X POST http://localhost:8080/api/v1/ingestions \
  -H "Content-Type: application/json" \
  -d '{"fileLocation":"samples/employees.csv","controlFileLocation":"employees_csv.properties","fileType":"CSV"}'
```

Windows PowerShell:
```powershell
$body = @{ fileLocation = "samples/employees.csv"; controlFileLocation = "employees_csv.properties"; fileType = "CSV" } | ConvertTo-Json
Invoke-RestMethod -Uri http://localhost:8080/api/v1/ingestions -Method Post -ContentType "application/json" -Body $body
```

The response (`202 Accepted`) contains the `executionId`. Samples:

| File | Control file | Expected result |
|---|---|---|
| `samples/employees.csv` | `employees_csv.properties` | SUCCESS, 20 loaded |
| `samples/employees.json` | `employees_json.properties` | SUCCESS, 20 loaded |
| `samples/employees_with_errors.csv` | `employees_with_errors.properties` | FAILED, 16 loaded, 4 errors (count mismatch) |

## 5. Check the result

| Endpoint | Purpose |
|---|---|
| `GET /api/v1/ingestions/{executionId}` | Status, expected vs loaded count, number of error records |
| `GET /api/v1/employees/{employeeId}` | Employee with phone, salary and emergency phone decrypted |
| `POST /api/v1/decrypt` `{"encryptedValue":"v1:..."}` | Decrypt one value copied from the database |

Rejected records are written to `data/error/execution-<executionId>.txt` and the `ingestion_error` table.

## Configuration

| Setting | Default | Where |
|---|---|---|
| Split threshold (bytes) | `1` (every file is split) | `LUMI_FILE_SIZE_THRESHOLD_BYTES` or `application.yml` |
| Records per split file | `50` | `LUMI_RECORDS_PER_SPLIT_FILE` (Airflow env) |
| Data / control file roots | `../data`, `../control-files` | `LUMI_LOCAL_DATA_ROOT`, `LUMI_LOCAL_CONTROL_FILE_ROOT` |

## Tests

153 tests in total. Database tests use the Docker warehouse and are skipped (not failed) when it is not running.

| Module | Tests | Command |
|---|---|---|
| Beam | 78 (incl. 7 database) | `cd beam-ingestion && ./mvnw test` |
| Spring Boot | 52 (incl. 4 database) | `cd springboot-ingestion-service && ./mvnw test` |
| Airflow DAG | 15 | `docker exec lumi-airflow-scheduler python -m unittest discover -s /opt/airflow/tests -v` |
| PySpark | 8 | `docker exec lumi-airflow-scheduler python -m unittest discover -s /opt/lumi/pyspark/tests -v` |

On Windows Git Bash, put `MSYS_NO_PATHCONV=1` before the `docker exec` commands so the container paths are not rewritten.

## Stop / reset

```bash
docker compose down        # stop
docker compose down -v     # stop and delete the database (tables are recreated on next start)
docker compose logs -f airflow-scheduler
```
