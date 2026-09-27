package com.amex.lumi.beam.model;

import java.io.Serializable;
import java.time.Instant;

/**
 * A valid employee plus metadata and the source file's last-modified time.
 */
public record EnrichedEmployee(
        ParsedEmployee parsed,
        String executionId,
        Instant ingestionTimestamp,
        Instant sourceModifiedAt) implements Serializable {

    /** Source file time unknown: the row always replaces the stored one. */
    public EnrichedEmployee(ParsedEmployee parsed, String executionId, Instant ingestionTimestamp) {
        this(parsed, executionId, ingestionTimestamp, null);
    }

    public EmployeeRecord employee() {
        return parsed.employee();
    }

    public Instant sourceCreationTime() {
        return parsed.sourceCreationTime();
    }
}
