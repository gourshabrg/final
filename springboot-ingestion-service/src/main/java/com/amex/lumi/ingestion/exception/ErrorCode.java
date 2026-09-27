package com.amex.lumi.ingestion.exception;

import org.springframework.http.HttpStatus;

/**
 * Stable error codes sent as "code" in error responses, each with its HTTP status.
 */
public enum ErrorCode {

    INVALID_REQUEST(HttpStatus.BAD_REQUEST),
    VALIDATION_FAILED(HttpStatus.BAD_REQUEST),
    MALFORMED_BODY(HttpStatus.BAD_REQUEST),
    INVALID_PATH_VALUE(HttpStatus.BAD_REQUEST),
    DECRYPTION_FAILED(HttpStatus.BAD_REQUEST),
    NOT_FOUND(HttpStatus.NOT_FOUND),
    ENDPOINT_NOT_FOUND(HttpStatus.NOT_FOUND),
    METHOD_NOT_ALLOWED(HttpStatus.METHOD_NOT_ALLOWED),
    UNSUPPORTED_MEDIA_TYPE(HttpStatus.UNSUPPORTED_MEDIA_TYPE),
    AIRFLOW_UNAVAILABLE(HttpStatus.BAD_GATEWAY),
    SERVER_DATA_ERROR(HttpStatus.INTERNAL_SERVER_ERROR),
    INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR);

    private final HttpStatus status;

    ErrorCode(HttpStatus status) {
        this.status = status;
    }

    public HttpStatus status() {
        return status;
    }
}
