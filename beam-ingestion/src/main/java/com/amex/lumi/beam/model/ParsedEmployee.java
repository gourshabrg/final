package com.amex.lumi.beam.model;

import java.io.Serializable;
import java.time.Instant;

/**
 * An employee read from a file, with its line number, file name and parse time.
 */
public record ParsedEmployee(
        long recordNumber,
        String sourceFile,
        Instant sourceCreationTime,
        EmployeeRecord employee) implements Serializable {
}
