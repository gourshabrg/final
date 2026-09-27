package com.amex.lumi.ingestion.exception;

/**
 * Airflow could not be reached or refused the DAG run. Returns 502.
 */
public class AirflowTriggerException extends LumiException {

    public AirflowTriggerException(String message, Throwable cause) {
        super(ErrorCode.AIRFLOW_UNAVAILABLE, message, cause);
    }
}
