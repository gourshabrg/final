package com.amex.lumi.ingestion.exception;

/**
 * Value is not valid ciphertext or was encrypted with another key. Returns 400.
 */
public class DecryptionException extends RuntimeException {

    public DecryptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
