\connect warehouse

CREATE TABLE IF NOT EXISTS employee (
    employee_id VARCHAR(7) PRIMARY KEY,

    first_name VARCHAR(15) NOT NULL,

    last_name VARCHAR(15),

    email VARCHAR(30) NOT NULL,

    phone_number_encrypted TEXT,

    hire_date DATE,

    department VARCHAR(20),

    job_title VARCHAR(30),

    salary_encrypted TEXT,

    currency CHAR(3),

    employment_status VARCHAR(13),

    manager_id VARCHAR(20),

    is_active BOOLEAN,

    skills JSONB,

    address JSONB,

    emergency_contact JSONB,

    ingestion_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,

    execution_id UUID NOT NULL,

    source_creation_time TIMESTAMP WITH TIME ZONE NOT NULL
);


CREATE TABLE IF NOT EXISTS ingestion_error (
    id BIGSERIAL PRIMARY KEY,

    execution_id UUID NOT NULL,

    source_file VARCHAR(1000) NOT NULL,

    record_number BIGINT,

    error_type VARCHAR(100),

    error_message TEXT NOT NULL,

    raw_record JSONB,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
