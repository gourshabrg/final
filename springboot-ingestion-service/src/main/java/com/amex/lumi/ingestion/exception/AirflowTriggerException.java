package com.amex.lumi.ingestion.exception;

public class AirflowTriggerException
        extends RuntimeException {

    public AirflowTriggerException(
            String message,
            Throwable cause) {

        super(message, cause);
    }
}
