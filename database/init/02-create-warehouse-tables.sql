\connect warehouse

CREATE TABLE IF NOT EXISTS employee (
    employee_id VARCHAR(7) PRIMARY KEY,

    first_name VARCHAR(15) NOT NULL,

    last_name VARCHAR(15),

    -- One email per employee; a second employee with the same email is rejected as a load error.
    email VARCHAR(30) NOT NULL CONSTRAINT uq_employee_email UNIQUE,

    phone_number_encrypted TEXT,

    hire_date DATE,

    department VARCHAR(20),

    job_title VARCHAR(30),

    salary_encrypted TEXT,

    currency CHAR(3),

    employment_status VARCHAR(13),

    manager_id VARCHAR(7),

    is_active BOOLEAN,

    skills JSONB,

    address JSONB,

    emergency_contact JSONB,

    ingestion_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,

    execution_id UUID NOT NULL,

    source_creation_time TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Last-modified time of the source file; an older file never overwrites a row from a newer one.
    source_modified_at TIMESTAMP WITH TIME ZONE
);


CREATE TABLE IF NOT EXISTS ingestion_error (
    id BIGSERIAL PRIMARY KEY,

    execution_id UUID NOT NULL,

    source_file VARCHAR(1000) NOT NULL,

    -- PySpark part file the record was read from; NULL when the file was not split.
    split_file VARCHAR(1000),

    record_number BIGINT,

    error_type VARCHAR(100),

    error_message TEXT NOT NULL,

    raw_record JSONB,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
