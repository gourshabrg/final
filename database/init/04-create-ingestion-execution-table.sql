\connect warehouse

CREATE TABLE IF NOT EXISTS ingestion_execution (
    execution_id UUID PRIMARY KEY,
    source_file VARCHAR(1000) NOT NULL,
    status VARCHAR(30) NOT NULL,
    expected_record_count BIGINT,
    actual_loaded_record_count BIGINT,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    failure_reason TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_ingestion_execution_status
        CHECK (
            status IN (
                'STARTED',
                'RUNNING',
                'SUCCESS',
                'FAILED'
            )
        )
);
