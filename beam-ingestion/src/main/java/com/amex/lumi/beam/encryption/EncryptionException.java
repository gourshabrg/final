package com.amex.lumi.beam.encryption;

/**
 * Thrown when a value cannot be encrypted or decrypted.
 */
public class EncryptionException extends RuntimeException {

    public EncryptionException(String message) {
        super(message);
    }

    public EncryptionException(String message, Throwable cause) {
        super(message, cause);
    }
}
