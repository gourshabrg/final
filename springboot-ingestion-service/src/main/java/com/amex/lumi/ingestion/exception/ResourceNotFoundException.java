package com.amex.lumi.ingestion.exception;

/**
 * Requested employee or execution does not exist. Returns 404.
 */
public class ResourceNotFoundException extends RuntimeException {

    public ResourceNotFoundException(String message) {
        super(message);
    }
}
