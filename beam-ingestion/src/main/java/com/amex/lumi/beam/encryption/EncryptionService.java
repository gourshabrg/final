package com.amex.lumi.beam.encryption;

/**
 * Contract for encrypting sensitive employee fields and decrypting values
 * for controlled application workflows.
 */
public interface EncryptionService {

    /**
     * Encrypts plaintext without exposing the value in logs or exceptions.
     *
     * @param plainText value to encrypt
     * @return encoded encrypted value
     */
    String encrypt(String plainText);

    /**
     * Decrypts a previously encoded encrypted value.
     *
     * @param encryptedText encoded value to decrypt
     * @return decrypted plaintext
     */
    String decrypt(String encryptedText);
}
