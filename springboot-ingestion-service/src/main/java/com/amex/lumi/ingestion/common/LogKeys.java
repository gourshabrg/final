package com.amex.lumi.ingestion.common;

/**
 * Names of the values added to every log line (see logging.pattern in application.yml).
 */
public final class LogKeys {

    public static final String REQUEST_ID = "requestId";
    public static final String EXECUTION_ID = "executionId";

    private LogKeys() {
    }
}
