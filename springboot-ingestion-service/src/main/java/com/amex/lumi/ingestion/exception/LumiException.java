package com.amex.lumi.ingestion.exception;

/**
 * Base of the API's exceptions; its ErrorCode sets the HTTP status and "code".
 */
public abstract class LumiException extends RuntimeException {

    private final ErrorCode code;

    protected LumiException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}
