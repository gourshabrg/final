package com.amex.lumi.ingestion.exception;

/**
 * Airflow could not be reached or refused the DAG run. Returns 502.
 */
public class AirflowTriggerException extends RuntimeException {

    public AirflowTriggerException(String message, Throwable cause) {
        super(message, cause);
    }
}
