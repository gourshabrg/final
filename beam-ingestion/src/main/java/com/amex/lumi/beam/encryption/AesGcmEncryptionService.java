package com.amex.lumi.beam.encryption;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Encrypts values with AES-256-GCM. The result is text like {@code v1:<iv>:<encrypted value>}.
 * A new random IV (starting value) is used every time, so the same salary never looks the same twice.
 * The Spring Boot API decrypts with the same key.
 */
public class AesGcmEncryptionService implements EncryptionService {

    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String VERSION = "v1";
    private static final int KEY_SIZE_BYTES = 32;
    private static final int IV_SIZE_BYTES = 12;
    private static final int TAG_LENGTH_BITS = 128;

    private final SecretKeySpec secretKey;
    private final SecureRandom secureRandom = new SecureRandom();

    public AesGcmEncryptionService(byte[] key) {
        if (key == null || key.length != KEY_SIZE_BYTES) {
            throw new IllegalArgumentException("AES-256 key must be exactly " + KEY_SIZE_BYTES + " bytes");
        }
        this.secretKey = new SecretKeySpec(key, ALGORITHM);
    }

    /** Key must be 32 characters (32 bytes). */
    public static AesGcmEncryptionService fromKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Encryption key must not be blank");
        }
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        if (bytes.length != KEY_SIZE_BYTES) {
            throw new IllegalArgumentException("LUMI_ENCRYPTION_KEY must be exactly " + KEY_SIZE_BYTES
                    + " bytes but is " + bytes.length);
        }
        return new AesGcmEncryptionService(bytes);
    }

    @Override
    public String encrypt(String plainText) {
        if (plainText == null) {
            throw new IllegalArgumentException("plainText must not be null");
        }
        try {
            byte[] iv = new byte[IV_SIZE_BYTES];
            secureRandom.nextBytes(iv);

            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(TAG_LENGTH_BITS, iv));
            byte[] cipherText = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));

            Base64.Encoder base64 = Base64.getEncoder();
            return VERSION + ":" + base64.encodeToString(iv) + ":" + base64.encodeToString(cipherText);
        } catch (GeneralSecurityException exception) {
            throw new EncryptionException("Unable to encrypt value", exception);
        }
    }

    @Override
    public String decrypt(String encryptedText) {
        if (encryptedText == null || encryptedText.isBlank()) {
            throw new IllegalArgumentException("encryptedText must not be null or blank");
        }
        try {
            String[] parts = encryptedText.split(":", -1);
            if (parts.length != 3 || !VERSION.equals(parts[0])) {
                throw new IllegalArgumentException("Value is not in the v1:<iv>:<ciphertext> format");
            }
            byte[] iv = Base64.getDecoder().decode(parts[1]);
            if (iv.length != IV_SIZE_BYTES) {
                throw new IllegalArgumentException("Invalid IV length");
            }
            byte[] cipherText = Base64.getDecoder().decode(parts[2]);

            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(TAG_LENGTH_BITS, iv));
            return new String(cipher.doFinal(cipherText), StandardCharsets.UTF_8);
        } catch (GeneralSecurityException | IllegalArgumentException exception) {
            throw new EncryptionException("Unable to decrypt value", exception);
        }
    }
}
