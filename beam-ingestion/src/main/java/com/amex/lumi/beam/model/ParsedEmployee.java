package com.amex.lumi.beam.model;

import java.io.Serializable;
import java.time.Instant;

/**
 * An employee read from a file; splitFile is the PySpark part file, if any.
 */
public record ParsedEmployee(
        long recordNumber,
        String sourceFile,
        String splitFile,
        Instant sourceCreationTime,
        EmployeeRecord employee) implements Serializable {

    /** File was not split, so there is no part file. */
    public ParsedEmployee(long recordNumber, String sourceFile, Instant sourceCreationTime, EmployeeRecord employee) {
        this(recordNumber, sourceFile, null, sourceCreationTime, employee);
    }
}
