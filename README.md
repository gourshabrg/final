# Amex Lumi Ingestion

Local ingestion platform using Spring Boot, Apache Airflow, PySpark, Apache Beam, and PostgreSQL.

## Prerequisites

- Docker Desktop or Docker Engine with the Compose plugin
- Java 17
- Maven 3.9 or newer
- Internet access for the first Maven and Docker builds

The Spring Boot module includes a Maven Wrapper. The Beam module requires Maven to be installed globally.

## Setup

Run from the repository root. On Windows use PowerShell. On macOS/Linux use `cp` instead of `Copy-Item` and `./mvnw` instead of `mvnw.cmd` for the Spring Boot module.

```powershell
Copy-Item .env.example .env
```

Set a private `LUMI_ENCRYPTION_KEY` in `.env`. Never commit `.env`.

Build the Beam shaded JAR. Airflow mounts `beam-ingestion/target`, and the DAG expects `beam-ingestion-1.0.0-SNAPSHOT.jar` there.

```powershell
Set-Location beam-ingestion
mvn clean package
Set-Location ..
docker compose up -d --build
```

Airflow is available at http://localhost:8082 with username `airflow` and password `airflow`.

PostgreSQL is available at `localhost:5432`; the warehouse database uses username `airflow` and password `airflow`.

## Start the API

In a second terminal at the repository root:

```powershell
Set-Location springboot-ingestion-service
.\mvnw.cmd spring-boot:run
```

The API runs at `http://localhost:8080`. The default local roots are `../data` and `../control-files`, so no machine-specific Windows path is required. Override them with `LUMI_LOCAL_DATA_ROOT` and `LUMI_LOCAL_CONTROL_FILE_ROOT` if necessary.

Check service health:

```powershell
Invoke-RestMethod http://localhost:8080/api/v1/ingestions/health
```

## Run an ingestion

Place an input file in `data/input` and its control file in `control-files`. Supported file types are `JSON`, `CSV`, `XML`, and `FIXED_WIDTH`.

```powershell
$body = @{
  fileLocation = "data/input/employees.csv"
  controlFileLocation = "control-files/employees.properties"
  fileType = "CSV"
} | ConvertTo-Json

Invoke-RestMethod `
  -Uri http://localhost:8080/api/v1/ingestions `
  -Method Post `
  -ContentType "application/json" `
  -Body $body
```

The API returns `202 Accepted` with an execution ID and triggers the `lumi_ingestion_orchestrator` DAG. Monitor the run in Airflow.

## Stop and reset

```powershell
docker compose down
```

Remove the PostgreSQL volume too when a clean database initialization is required:

```powershell
docker compose down -v
```

Useful diagnostics:

```powershell
docker compose ps
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver
```

Run tests:

```powershell
Set-Location beam-ingestion
mvn test
Set-Location ..
Set-Location springboot-ingestion-service
.\mvnw.cmd test
Set-Location ..
```

Generated build output, Airflow logs, local data, and `.env` are excluded by the root `.gitignore`.