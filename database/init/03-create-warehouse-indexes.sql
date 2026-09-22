\connect warehouse

CREATE INDEX IF NOT EXISTS idx_employee_execution_id
    ON employee (execution_id);

CREATE INDEX IF NOT EXISTS idx_ingestion_error_execution_id
    ON ingestion_error (execution_id);
