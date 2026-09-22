package com.amex.lumi.beam.model;

import java.io.Serializable;
import java.time.Instant;

/**
 * A valid employee plus the metadata columns execution_id and ingestion_timestamp.
 */
public record EnrichedEmployee(
        ParsedEmployee parsed,
        String executionId,
        Instant ingestionTimestamp) implements Serializable {

    public EmployeeRecord employee() {
        return parsed.employee();
    }

    public Instant sourceCreationTime() {
        return parsed.sourceCreationTime();
    }
}
