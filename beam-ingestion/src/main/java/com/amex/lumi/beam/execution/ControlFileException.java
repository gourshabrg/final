package com.amex.lumi.beam.execution;

/**
 * Control file missing, invalid or not matching the data file; the run stops before reading data.
 */
public class ControlFileException extends RuntimeException {

    public ControlFileException(String message) {
        super(message);
    }

    public ControlFileException(String message, Throwable cause) {
        super(message, cause);
    }
}
