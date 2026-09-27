package com.amex.lumi.ingestion.exception;

/**
 * Client sent something we cannot process (missing file, bad control file ...). Returns 400.
 */
public class InvalidRequestException extends LumiException {

    public InvalidRequestException(String message) {
        super(ErrorCode.INVALID_REQUEST, message, null);
    }
}
