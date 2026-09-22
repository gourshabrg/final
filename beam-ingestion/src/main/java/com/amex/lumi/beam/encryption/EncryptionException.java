package com.amex.lumi.beam.encryption;

/**
 * Indicates that an encryption or decryption operation failed.
 */
public class EncryptionException
        extends RuntimeException {

    public EncryptionException(
            String message,
            Throwable cause) {

        super(message, cause);
    }
}
