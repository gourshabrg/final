# Lumi Ingestion: Interview Guide

This guide explains how the project works, why each piece exists, and how to talk about it in an interview.
Read it top to bottom once, then use the Q&A section to practise.

---

## 1. The project in one minute

A company receives employee data files (called **SOR feeds**, "System of Record") from other systems.
Those files must be loaded into a **data warehouse** so analysts can query them.
The project does that, in a way that is:

- **safe**: bad records never stop good records from loading, and sensitive fields are encrypted
- **traceable**: every run has an ID, a status and an error report
- **scalable**: large files are split and processed in parallel

**Elevator pitch to memorise:**
> "A Spring Boot API receives an ingestion request, validates the file and control file, creates an execution ID
> and triggers an Airflow DAG through Airflow's REST API. The DAG splits large files with PySpark, then runs an
> Apache Beam job that parses CSV or JSON, validates every record, fills missing values, adds metadata columns,
> encrypts sensitive fields with AES-256-GCM and loads PostgreSQL. Bad records go to an error file and table.
> At the end Beam compares the loaded count with the control file and fails the run if they differ."

---

## 2. Architecture

```
          (1) POST /api/v1/ingestions
Client ──────────────────────────────▶ Spring Boot API (port 8080)
                                          │ validate file + control file
                                          │ create executionId (UUID)
                                          │ decide: split needed?
                                          ▼ (2) POST /api/v1/dags/.../dagRuns
                                       Airflow (port 8082)
                                          │
        ┌─────────────────────────────────┼───────────────────────────────┐
        ▼                                 ▼                               ▼
 log_request → validate_request → choose_split ─┬─ run_pyspark_split ─┐
                                                └─ skip_pyspark_split ─┴─ split_join
                                                                              │
                                        run_beam_pipeline ◀───────────────────┘
                                              │  (java -jar beam-ingestion.jar)
                                              ▼
                        read → validate → fill missing → metadata → encrypt → load
                              │                                              │
                              └──── failures ──▶ error .txt + ingestion_error │
                                                                             ▼
                                                              count check → ingestion_execution
                                              │
                         check_execution_status → finalize
```

**Where things run**

| Component | Runs where | Why |
|---|---|---|
| Spring Boot API | Your machine (host) | Easy to develop and debug |
| PostgreSQL | Docker container | Same database for everyone, no install |
| Airflow webserver + scheduler | Docker containers | Airflow does not run natively on Windows |
| PySpark + Beam | Inside the Airflow scheduler container | Airflow starts them as tasks |

Folders are shared between host and containers with **Docker volumes**. That is why the API converts
`C:\...\final\data\samples\employees.csv` into `/opt/lumi/data/samples/employees.csv` before sending it to Airflow.

---

## 3. Why each technology

| Technology | Why it is used here | How to say it in an interview |
|---|---|---|
| **Spring Boot** | Quick REST API with validation, error handling, config | "Entry point. It keeps bad requests away from the expensive pipeline." |
| **Apache Airflow** | Orchestrates steps in order, retries, shows each task's log | "Airflow is the conductor: it doesn't process data, it decides what runs when." |
| **PySpark** | Splits big files in parallel | "Spark is good at reading a huge file once and writing N smaller files." |
| **Apache Beam** | Record-level processing with branches (valid / invalid) | "Beam lets me write the pipeline once and run it locally (DirectRunner) or on Google Dataflow." |
| **PostgreSQL** | Stands in for BigQuery | "Warehouse stand-in; the requirement allowed Postgres to avoid cloud cost." |
| **Docker Compose** | Starts Postgres + Airflow with one command | "Anyone can run the platform the same way." |

**GCP mapping (often asked):** Airflow → **Cloud Composer**, Beam DirectRunner → **Dataflow**,
PySpark → **Dataproc**, local folders → **GCS**, PostgreSQL → **BigQuery**, `.env` key → **Secret Manager**.

---

## 4. One request, step by step

1. **Client** sends:
   ```json
   {"fileLocation":"samples/employees.csv","controlFileLocation":"employees_csv.properties","fileType":"CSV"}
   ```
2. **Controller** (`IngestionController`) checks the JSON fields with `@Valid` (not blank, fileType is CSV or JSON).
3. **IngestionService** runs the business checks:
   - `IngestionPathResolver` turns the relative path into a full path and **rejects paths outside `data/`**
     (so `../../secret.txt` cannot be read).
   - `SourceFileValidator` checks the file exists, is readable, not empty, and has the right extension.
   - `ControlFileValidator` checks `record_count` exists and is a non-negative number.
4. The service creates the **execution ID** with `UUID.randomUUID()`.
5. **Split decision**: `fileSize > threshold`. The threshold is in `application.yml` (Phase 2 requirement).
6. `DagRunRequestFactory` builds `dag_run.conf` with the container paths, the split folder and the error file prefix.
7. `AirflowRestClient` POSTs to Airflow. The API immediately returns **202 Accepted** with the execution ID.
8. **Airflow** runs the DAG tasks (section 7).
9. **Beam** loads the data and writes the final status (section 6).
10. The client checks `GET /api/v1/ingestions/{executionId}`.

Why **202** and not 200? Because the work is not finished when we answer; it continues in Airflow.

---

## 5. Spring Boot module, file by file

```
controller/  IngestionController, EmployeeController, DecryptionController
service/     IngestionService, IngestionPathResolver, SourceFileValidator, ControlFileValidator,
             DagRunRequestFactory, EmployeeService
client/      AirflowClient (interface), AirflowRestClient, AirflowDagRunRequest
repository/  IngestionExecutionRepository, EmployeeRepository, EmployeeRow
security/    FieldDecryptor
config/      IngestionProperties, AirflowProperties, EncryptionProperties
dto/         request/response records
exception/   GlobalExceptionHandler + 4 exception types
logging/     RequestLoggingFilter
```

**Layering rule:** controller → service → repository/client. Controllers never talk to the database directly.

### 5.1 `IngestionService.startIngestion`, line by line

```java
Path dataFile = paths.resolveDataFile(request.fileLocation());        // relative -> absolute, must stay inside data/
Path controlFile = paths.resolveControlFile(request.controlFileLocation());
long fileSize = sourceFileValidator.validate(dataFile, request.fileType()); // exists, readable, extension, not empty
long expectedRecords = controlFileValidator.validate(controlFile);    // record_count must be a number >= 0

UUID executionId = UUID.randomUUID();                                  // one ID for the whole run (requirement)
boolean requiresSplit = fileSize > thresholdBytes;                     // Phase 2: big files are split first
MDC.put(LogKeys.EXECUTION_ID, executionId.toString());                 // every log line now shows this ID
try {
    AirflowDagRunRequest dagRun = dagRunRequestFactory.create(new RunDetails(...)); // build dag_run.conf
    LOGGER.info("Ingestion accepted: ...");                           // one summary log line
    airflowClient.triggerDag(dagRun);                                  // REST call to Airflow
    return new IngestionResponse(executionId, ..., "ACCEPTED", ...);
} finally {
    MDC.remove(LogKeys.EXECUTION_ID);                                  // threads are reused; always clean up
}
```

**Why validate here if Beam validates too?** Different levels. The API checks the *request* (does the file exist?).
Beam checks the *records* (is this email valid?). Failing fast in the API saves an Airflow run.

### 5.2 Other important classes

| Class | What to say |
|---|---|
| `IngestionPathResolver` | Maps host paths to container paths. Uses `normalize()` + `startsWith(root)` to block path traversal. |
| `DagRunRequestFactory` | Builds `dag_run.conf`. Spring passes `split_output_dir` to the DAG, which is the Phase 2 requirement "pass the split location as DAG parameters". |
| `AirflowRestClient` | `RestClient` with basic auth and **timeouts**. Without timeouts, a hung Airflow would block API threads forever. |
| `AirflowClient` (interface) | Lets tests replace Airflow with a mock (`IngestionServiceTest`). |
| `GlobalExceptionHandler` | `@RestControllerAdvice`: one place that maps exceptions to 400 / 404 / 502 / 500 with the same JSON shape. |
| `RequestLoggingFilter` | Adds a `requestId` to every log line and logs `POST /api/v1/ingestions -> 202 (35 ms)`. |
| `*Properties` records | `@ConfigurationProperties` + `@Validated`: the app refuses to start with missing config. |
| `FieldDecryptor` | AES-256-GCM decryption. Checks the key is **exactly 32 bytes at startup** (fail fast). |
| `EmployeeService` | Reads the row, decrypts phone / salary / emergency phone, logs *who* was decrypted (never the values). |

### 5.3 HTTP status codes used

| Code | When |
|---|---|
| 202 | Ingestion accepted |
| 400 | Bad JSON, missing field, file not found, bad control file, value cannot be decrypted |
| 404 | Unknown execution ID or employee |
| 502 | Airflow unreachable or refused the run ("bad gateway": the problem is in a system behind us) |
| 500 | Anything unexpected (details only in the log, never in the response) |

---

## 6. Beam module, file by file

```
pipeline/   EmployeeIngestionPipeline (main), IngestionGraph (wires the steps)
options/    IngestionPipelineOptions (--inputFile, --fileType ...)
read/       ReadEmployeeFiles, CsvEmployeeParser, JsonEmployeeParser, EmployeeFileParser, RecordHandler
validate/   EmployeeValidator (rules), ValidateEmployees (Beam step)
transform/  MissingValueCleanser, PrepareForWarehouse (fill → metadata → encrypt)
write/      LoadEmployees, EmployeeStatementBinder, WriteErrorReport, DatabaseConfig, JdbcSupport, ...
execution/  RecordCountCheck, ExecutionStatusRepository, ControlFileReader
model/      records: ParsedEmployee, EnrichedEmployee, EncryptedEmployee, RecordFailure, ...
common/     IngestionMetrics (counters)
```

### 6.1 Beam vocabulary (learn these five words)

| Term | Meaning |
|---|---|
| **Pipeline** | The whole job graph |
| **PCollection** | A (possibly huge) collection of elements flowing between steps |
| **PTransform** | A step: takes PCollection(s), returns PCollection(s). Our composite steps extend it. |
| **DoFn / ParDo** | "Do this for each element". `ParDo.of(new MyFn())` applies a DoFn in parallel. |
| **TupleTag** | A label for one of several outputs, used to split valid / invalid records. |

Also: **Runner**, which executes the graph. We use **DirectRunner** (local). The same code runs on **Dataflow**.

### 6.2 `EmployeeIngestionPipeline.main`, line by line

```java
TimeZone.setDefault(TimeZone.getTimeZone("UTC"));            // timestamps are the same on every machine
IngestionPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()                                     // fails if a @Validation.Required arg is missing
        .as(IngestionPipelineOptions.class);

IngestionControl control = new ControlFileReader().read(...); // read record_count BEFORE touching data
DatabaseConfig database = new DatabaseConfig(url, user, pwd);
RecordCountCheck.RunContext run = new RunContext(executionId, input, expected, Instant.now());

statusRepository.save(IngestionExecution.started(...));      // STARTED row
Pipeline pipeline = Pipeline.create(options);
IngestionGraph.build(pipeline, options, database, run);       // describe the steps (nothing runs yet)
statusRepository.save(IngestionExecution.running(...));      // RUNNING row
runAndWait(...)                                               // run, wait, print summary, handle crash
```

Important: `IngestionGraph.build` only **describes** the graph. Beam executes it at `pipeline.run()`.
This is called **deferred execution**, and interviewers like to ask about it.

`runAndWait` catches any crash and calls `markFailedIfUnfinished`, so a run never stays stuck at `RUNNING`.

### 6.3 `IngestionGraph.build`, the heart of the job

```java
PCollectionTuple read = pipeline.apply("ReadEmployees", new ReadEmployeeFiles(input, fileType, id));
PCollectionTuple validated = read.get(PARSED).apply("ValidateEmployees", new ValidateEmployees(id));
PCollection<EncryptedEmployee> rows = validated.get(VALID)
        .apply("PrepareForWarehouse", new PrepareForWarehouse(id, key));
PCollectionTuple loaded = rows.apply("LoadEmployees", new LoadEmployees(database));

PCollectionList.of(read.get(PARSE_FAILURES))                  // 3 kinds of failures ...
        .and(validated.get(INVALID))
        .and(loaded.get(FAILED))
        .apply(Flatten.pCollections())                        // ... merged into one collection
        .apply(new WriteErrorReport(errorOutput, database));  // → .txt file + ingestion_error table

loaded.get(LOADED).apply(new RecordCountCheck(run, database)); // Phase 3
```

This is the **dead-letter pattern**: records that fail go down a side branch instead of crashing the job.
That is exactly the requirement "the entire ingestion should not fail in case of partial success".

### 6.4 Reading: `ReadEmployeeFiles` + parsers

- `FileIO.match().filepattern(...)` supports a single file **or a glob** (`/split/<id>/*.csv`), so the same
  code reads the original file or PySpark's split files.
- `ParseFileFn` numbers records 1, 2, 3 ... per file, which is needed for clear error reports.
- It **skips the BOM** (an invisible first character added by Excel on Windows). Without this, the first
  CSV header would be `\uFEFFemployee_id` and every row would fail.
- `source_creation_time` = the moment the file was parsed (the requirement's definition).
- Parsers implement one interface (`EmployeeFileParser`). Adding a format = one new class + one line in
  `EmployeeFileParsers` (Open/Closed Principle).

**CSV parser:** uses Apache Commons CSV, not `split(",")`, because values like `"102, Silicon Heights"`
contain commas. `is_active` accepts only `true`/`false`, since `Boolean.valueOf("yes")` would silently give `false`.

**JSON parser:** streams one object at a time with Jackson's `JsonParser` instead of loading the whole array.
Benefits: flat memory, and a wrongly typed field rejects only that record. It supports both a JSON array
and **JSON Lines** (one object per line), which is what PySpark writes when it splits JSON.

### 6.5 Validation rules (`EmployeeValidator`)

| Field | Rule |
|---|---|
| employee_id, manager_id | required, exactly 7 characters |
| first_name | required, 3–15 |
| last_name | optional, max 15 |
| email | required, 13–30, valid format |
| phone_number | required, exactly 10 characters |
| hire_date | required, 10 characters, real date `yyyy-MM-dd` |
| department / job_title | optional, max 20 / 30 |
| currency | required, 3 uppercase letters |
| employment_status | required, 3–13 |
| is_active | required boolean |
| skills | all skills joined ≤ 100 characters |
| salary | not negative |

Design: rules are a **table** (`List<LengthRule>`) instead of one method per field. Adding a rule is one line.
The validator returns **all** errors of a record at once, so the data provider can fix everything in one go.

### 6.6 Transform: `PrepareForWarehouse`

1. `ReplaceMissingValuesFn`: null/empty text → `" "` (Phase 1 rule). Salary, `is_active` and `hire_date`
   are skipped because number/boolean/date columns cannot hold a space. It returns a **copy**: Beam forbids
   changing an input element (the DirectRunner actually checks this).
2. `AddMetadataFn`: adds `execution_id` and `ingestion_timestamp` (source_creation_time was set at parse time).
3. `EncryptFieldsFn`: encrypts phone, salary, emergency phone. The cipher is created in `@Setup`, not in the
   constructor, because DoFns are **serialized** and sent to workers; the cipher object is not serializable (`transient`).

### 6.7 Loading: `LoadEmployees` + `EmployeeStatementBinder`

- **One connection per DoFn instance**, opened in `@Setup`, closed in `@Teardown`, not one per record.
- **One commit per row.** Slower than a batch, but one bad row can be rolled back alone.
- **UPSERT** (`INSERT ... ON CONFLICT (employee_id) DO UPDATE`): re-running a file updates rows instead of
  failing on duplicates. This makes the job **idempotent**.
- **Error classification:** SQLSTATE `22xxx` (bad data, e.g. value too long) and `23xxx` (constraint) → this row
  is bad → send to the failure branch. Anything else (DB down, wrong password) → throw → fail the job, because
  marking 10,000 rows as "bad" when the DB is simply down would be wrong.

### 6.8 Error report: `WriteErrorReport`

- Text file: `data/error/execution-<id>.txt`, one line per failure:
  `record_number=3|error_type=VALIDATION_ERROR|error_message=...|employee_id=...|source_file=...`
- Table: `ingestion_error`, with `raw_record` stored as JSONB **with phone and salary replaced by `[REDACTED]`**
  (`RedactedEmployeeJson`). Error tables are read by support staff and are not encrypted.

### 6.9 Phase 3: `RecordCountCheck`

```
loaded rows → Count.globally() → compare with record_count
     match    → save SUCCESS
     mismatch → save FAILED ─▶ Wait.on(saved) ─▶ throw RecordCountMismatchException
```

Why `Wait.on`? Beam steps run in parallel. If we threw immediately, the job could stop **before** the FAILED
row was committed, leaving the status stuck at RUNNING. `Wait.on` guarantees "save first, then fail".

### 6.10 Encryption: AES-256-GCM

Stored format: `v1:<IV base64>:<ciphertext+tag base64>`

- **AES-256**: symmetric encryption with a 32-byte key (same key in Beam and Spring).
- **GCM mode**: encrypts **and** authenticates. If someone edits the ciphertext, decryption fails
  (see test `tamperedValueIsRejected`).
- **Random 12-byte IV per value**: the same salary encrypted twice gives different ciphertext, so nobody can
  spot equal salaries in the table.
- **`v1:` prefix**: versioning, so the algorithm can change later without breaking old rows.

### 6.11 Logs and metrics

**Which log level when (same rule in every module):**

| Level | Used for | Example |
|---|---|---|
| DEBUG | Detail only needed while debugging | "Loaded employee_id=E0001" |
| INFO | Normal progress | "Control file expects 20 record(s)", "Execution is now SUCCESS" |
| WARN | A problem that does not stop the run | A record failed validation, a 400 response, rejected rows in a finished run |
| ERROR | Something failed and needs attention | Database down, record-count mismatch, Airflow unreachable, task failed |

Each problem is logged **once**, where it happens, with its reason. For example, the API's request filter
writes one INFO access line per request and the exception handler writes the WARN line with the reason.


Beam uses **counters** (`IngestionMetrics`) instead of one log line per record, then prints one summary:
`Ingestion summary executionId=... parsed=20 parse_errors=1 validation_errors=3 loaded=16 load_errors=0`.
On Dataflow the same counters appear in the monitoring UI.

---

## 7. Airflow DAG

`lumi_ingestion_orchestrator.py` contains **only the wiring**. Logic lives in `lumi_ingestion/tasks.py`
(Python) and `airflow/scripts/*.sh` (bash). Small files are easier to test and review.

| Task | Type | What it does |
|---|---|---|
| `log_request` | Python | Logs the request parameters |
| `validate_request` | Python | Required keys present, type is CSV/JSON, files visible **inside** the container |
| `choose_split` | Branch | Returns the next task id: `run_pyspark_split` or `skip_pyspark_split` |
| `run_pyspark_split` | Bash | Runs `split_file.py` |
| `skip_pyspark_split` | Empty | Placeholder for the "no split" path |
| `split_join` | Empty | Trigger rule `NONE_FAILED_MIN_ONE_SUCCESS`, so it runs after whichever branch ran |
| `run_beam_pipeline` | Bash | Picks original file or split glob, runs `java -jar` |
| `check_execution_status` | Python | Reads `ingestion_execution` with `PostgresHook`, fails if not SUCCESS, pushes counts to **XCom** |
| `finalize` | Python | Pulls counts from XCom and logs a summary |

Every log line starts with `[step] execution_id=...`, so you can follow one run by searching for its ID.

**Why the trailing space in `bash_command="bash .../run_beam_pipeline.sh "`?** If a BashOperator command ends
in `.sh`, Airflow tries to load it as a Jinja template file and fails. The space avoids that (a well-known gotcha).

**Why `env=` + `append_env=True`?** Values reach the script as environment variables (safer than building a
long command string), and `append_env` keeps the container's own variables such as `LUMI_ENCRYPTION_KEY`.

---

## 8. PySpark split job

- Reads CSV (all columns as text) or JSON (`multiLine` only for arrays; JSON Lines read line by line).
- `file_count = ceil(records / records_per_file)` → `repartition(file_count)` → write.
- Spark writes `part-*.csv` / `part-*.json` files in `data/split/<executionId>/`; Beam reads them with a glob.
- Output from an earlier attempt of the same run is deleted first, so retries are safe.

---

## 9. Database tables

| Table | One row per | Key columns |
|---|---|---|
| `employee` | employee | `employee_id` (PK), encrypted phone/salary, JSONB skills/address/emergency_contact, 3 metadata columns |
| `ingestion_error` | rejected record | execution_id, record_number, error_type, error_message, redacted raw_record |
| `ingestion_execution` | run | status, expected vs actual count, failure_reason, timings |

JSONB is used for nested data (address, emergency contact, skills) because it keeps the structure and can still
be queried: `SELECT address->>'city' FROM employee`.

---

## 10. Requirement → where it is implemented

| Requirement | Where |
|---|---|
| Ingest CSV / JSON | `CsvEmployeeParser`, `JsonEmployeeParser` (XML and fixed-width were dropped from scope) |
| Parsing with Beam | `ReadEmployeeFiles` |
| Replace null/missing with whitespace | `MissingValueCleanser` |
| 3 metadata columns | `ParseFileFn` (source_creation_time), `AddMetadataFn` (execution_id, ingestion_timestamp) |
| Bad records to an error file, no full failure | `WriteErrorReport` + dead-letter branches |
| Orchestration with Airflow | `lumi_ingestion_orchestrator.py` |
| Spring Boot API creates execution ID and triggers DAG | `IngestionService`, `AirflowRestClient` |
| PySpark split for large files | `split_file.py` |
| API checks file size vs configurable threshold | `DagRunRequestFactory` + `lumi.ingestion.file-size-threshold-bytes` |
| Split location passed as DAG parameter | `split_output_dir` in `dag_run.conf` |
| Control file with record_count | `ControlFileValidator` (API), `ControlFileReader` (Beam) |
| Fail on count mismatch with clear message | `RecordCountCheck` → "control file expected 20 record(s) but 16 were loaded" |
| Unit tests | 153 tests: 78 Beam, 52 Spring Boot, 15 Airflow, 8 PySpark (11 of them against the real database) |
| Decrypt stored fields | `GET /api/v1/employees/{id}`, `POST /api/v1/decrypt` |

---

## 11. What was changed in the refactor, and why

| Before | After | Why |
|---|---|---|
| `EmployeeIngestionPipeline` had 866 lines, mostly one method | ~90 lines + `IngestionGraph` + small PTransforms | God class, impossible to test or review |
| `WriteEmployeeToPostgresFn` had 593 lines | `LoadEmployees` + `EmployeeStatementBinder` + `JdbcSupport` | Single responsibility |
| JDBC connect/rollback/close copied into 3 classes | `DatabaseConfig` + `JdbcSupport` | DRY (don't repeat yourself) |
| 6 unused classes (`RecordCountValidator`, `JsonArrayToEmployeesFn`, ...) | Deleted | Dead code confuses readers |
| Two error formatters with copied `sanitize()` | One `ErrorLineFormatter`, one error file | Duplication |
| Hand-written equals/hashCode everywhere | Java `record`s | Less code, immutable by default |
| API config pointed to `C:/Users/Ravindra/...` | `../data` default + env overrides | Runs on any machine |
| Missing control file returned **500** | Returns **400** with a clear message | Correct HTTP semantics |
| Spring hashed short keys with SHA-256, Beam rejected them | Both require exactly 32 bytes | Keys could silently differ, making decryption impossible |
| Null-to-whitespace rule missing | `MissingValueCleanser` | Phase 1 requirement |
| Only max lengths checked | Full min/max table from the spec | Requirement |
| `is_active=yes` silently became `false` | Reported as a parse error | Silent data corruption |
| Bad salary in CSV crashed the whole file | Only that record fails | Partial success |
| JSON Lines read with `multiLine=true` in PySpark | Detects array vs lines | Bug on already-split input |
| DAG status check was an empty placeholder | Queries `ingestion_execution` | Real verification |
| Split location built inside the DAG | Passed by Spring as `split_output_dir` | Phase 2 requirement |
| Error table stored plaintext phone/salary risk | Redacted JSON | Data protection |
| No Maven wrapper for Beam | Added `mvnw` | No global Maven needed |
| Windows CRLF could break `.sh` in containers | `.gitattributes` forces LF | Cross-platform |
| 3,000-line generated `airflow.cfg` committed | Ignored; env vars in compose | Generated files don't belong in git |

---

## 12. Interview questions and answers

**Q1. Why Airflow and Beam? Isn't one enough?**
They do different jobs. Airflow **orchestrates** (order, retries, visibility), Beam **processes data**.
Airflow should never hold the data itself.

**Q2. What is a DoFn and why is `@Setup` used?**
A DoFn is per-element logic. DoFns are serialized and shipped to workers, so heavy or non-serializable objects
(DB connection, cipher) are created in `@Setup` once per instance and marked `transient`.

**Q3. How do you handle bad records?**
Multi-output `ParDo` with `TupleTag`s. Invalid records go to a failure branch (dead-letter), all failure branches
are `Flatten`ed and written to a text file and the `ingestion_error` table. Good records keep flowing.

**Q4. What happens if the database goes down mid-run?**
The SQLSTATE is not 22/23, so `LoadEmployees` rethrows, the job fails, and `markFailedIfUnfinished`
sets status FAILED with the root cause.

**Q5. Is the job idempotent?**
Yes for the employee table (UPSERT on employee_id). Re-running the same file updates rows instead of duplicating them.

**Q6. Why commit per row instead of batching?**
To isolate failures. With a batch, one bad row rolls back the whole batch. The trade-off is speed; for large
volumes you would batch and, on failure, retry that batch row by row.

**Q7. How is the record count check guaranteed to save FAILED before failing?**
`Wait.on(failureSaved)` delays the throwing step until the FAILED row is written.

**Q8. What's the difference between 200, 202 and 201?**
200 = done, 201 = created a resource, 202 = accepted but still processing. We return 202.

**Q9. How is the execution ID used?**
Created once in the API (UUID v4), passed to Airflow (`dag_run_id = ingestion_<id>`), to Beam, stored on every
row, every error and the execution table, and printed in every log line (MDC / log prefix). It ties everything together.

**Q10. Why AES-GCM and not AES-ECB/CBC?**
ECB leaks patterns. CBC has no integrity check. GCM gives confidentiality **and** integrity, and with a random IV
per value equal plaintexts look different.

**Q11. Where should the key live in production?**
In a secret manager (GCP Secret Manager / Vault), with key rotation. The `v1:` prefix supports rotation.

**Q12. Is the decrypt endpoint safe?**
Locally yes, but in production it must be protected (Spring Security + OAuth2 roles), and every decrypt should
be audited. We already log who was decrypted, never the values.

**Q13. How do you prevent path traversal in the API?**
`Path.normalize()` then `startsWith(root)`. `../../etc/passwd` resolves outside the root and is rejected.

**Q14. What does `@ConfigurationProperties` + `@Validated` give you?**
Type-safe config objects, validated at startup, so a missing Airflow URL fails immediately, not on the first request.

**Q15. How do you test code that calls Airflow?**
`AirflowClient` is an interface; tests use a Mockito mock and capture the request (`ArgumentCaptor`) to assert
`dag_run.conf`.

**Q16. What does `@WebMvcTest` do?**
Starts only the web layer (controller, advice, filters) with a mocked service. Fast, no DB, no Airflow.

**Q17. Why split large files if Beam is already parallel?**
One big file is read by one worker unless the format is splittable. Many smaller files = many parallel reads.
JSON arrays in particular cannot be split by Beam.

**Q18. What's a branch operator and a trigger rule?**
`BranchPythonOperator` returns the task id to run; the other branch is skipped. The join task needs
`NONE_FAILED_MIN_ONE_SUCCESS`, otherwise the default `ALL_SUCCESS` would skip it because one parent was skipped.

**Q19. What is XCom?**
Small messages between Airflow tasks. `check_execution_status` returns a dict; `finalize` pulls it.

**Q20. What is MDC?**
Mapped Diagnostic Context: per-thread key/values (requestId, executionId) that the log pattern prints automatically.

**Q21. Why records for DTOs and models?**
Immutable, concise, built-in equals/hashCode/toString. We override `toString` where it would print secrets.

**Q22. How would you move this to GCP?**
GCS for files, Composer for Airflow, Dataflow runner for Beam (`--runner=DataflowRunner`), Dataproc for PySpark,
BigQueryIO instead of JDBC, Secret Manager for the key, Cloud Logging for logs.

**Q23. What is `source_creation_time` vs `ingestion_timestamp`?**
Per the requirement, `source_creation_time` is when the data was parsed from the file;
`ingestion_timestamp` is when the row was prepared for loading into the warehouse.

**Q24. How is the project tested?**
Unit tests for every rule and parser, Beam pipeline tests with `PAssert`, `@WebMvcTest` for controllers,
a local HTTP server standing in for Airflow, Python `unittest` for the DAG and PySpark job, and integration
tests that write to the real PostgreSQL. The database tests skip themselves when the database is down,
so the build works on any machine.

**Q25. Why do the tests force `-Duser.timezone=UTC`?**
The PostgreSQL JDBC driver sends the machine's time zone when it logs in. Some machines report old names
like `Asia/Calcutta`, which the database rejects. Production code already runs in UTC; the tests now do too.

**Q26. What would you improve next?**
Batch writes with row-level fallback; authentication; retries/alerts in Airflow; data-quality metrics
dashboard; schema registry for input formats; integration tests with Testcontainers.

---

## 13. Demo script (5 minutes)

1. `docker compose ps`: Postgres and Airflow running.
2. `POST /api/v1/ingestions` with `samples/employees.csv` → show 202 and executionId.
3. Airflow UI → graph view → green tasks → open `run_beam_pipeline` log → summary line.
4. `GET /api/v1/ingestions/{id}` → SUCCESS 20/20.
5. `SELECT phone_number_encrypted FROM employee LIMIT 1` → ciphertext.
6. `GET /api/v1/employees/EMP0001` → decrypted values.
7. Run `employees_with_errors.csv` → FAILED, 16 loaded, open `data/error/execution-<id>.txt` → 4 clear reasons.
