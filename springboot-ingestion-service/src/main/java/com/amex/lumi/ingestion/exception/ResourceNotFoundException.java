package com.amex.lumi.ingestion.exception;

/**
 * Requested employee or execution does not exist. Returns 404.
 */
public class ResourceNotFoundException extends LumiException {

    public ResourceNotFoundException(String message) {
        super(ErrorCode.NOT_FOUND, message, null);
    }
}
