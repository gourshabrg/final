package com.amex.lumi.beam.execution;

/**
 * Loaded count differs from the control file's record_count.
 */
public class RecordCountMismatchException extends IllegalStateException {

    public RecordCountMismatchException(String message) {
        super(message);
    }
}
