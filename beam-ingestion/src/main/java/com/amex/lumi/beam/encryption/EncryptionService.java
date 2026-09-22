package com.amex.lumi.beam.encryption;

/**
 * Encrypts and decrypts sensitive fields (phone, salary).
 */
public interface EncryptionService {

    String encrypt(String plainText);

    String decrypt(String encryptedText);
}
