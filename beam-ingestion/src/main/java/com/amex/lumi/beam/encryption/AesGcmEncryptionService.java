package com.amex.lumi.beam.encryption;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * AES-256-GCM encryption as {@code v1:<iv>:<encrypted value>}, with a new random IV each time.
 */
public class AesGcmEncryptionService implements EncryptionService {

    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String VERSION = "v1";
    private static final int KEY_SIZE_BYTES = 32;
    private static final int BASE64_KEY_LENGTH = 44;
    private static final int IV_SIZE_BYTES = 12;
    private static final int TAG_LENGTH_BITS = 128;

    private final SecretKeySpec secretKey;
    private final SecureRandom secureRandom = new SecureRandom();

    private AesGcmEncryptionService(byte[] key) {
        this.secretKey = new SecretKeySpec(key, ALGORITHM);
    }

    /** Key: 32 random bytes in base64 (recommended, e.g. openssl rand -base64 32) or 32 typed characters. */
    public static AesGcmEncryptionService fromKey(String key) {
        return new AesGcmEncryptionService(keyBytes(key));
    }

    /** True for a 32-character typed key: it works, but has far fewer possible values than 32 random bytes. */
    public static boolean isTypedKey(String key) {
        return decodeBase64Key(key) == null;
    }

    private static byte[] keyBytes(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Encryption key must not be blank");
        }
        byte[] decoded = decodeBase64Key(key);
        if (decoded != null) {
            return decoded;
        }
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        if (bytes.length != KEY_SIZE_BYTES) {
            throw new IllegalArgumentException("LUMI_ENCRYPTION_KEY must be 32 bytes in base64 or exactly "
                    + KEY_SIZE_BYTES + " characters, but is " + bytes.length + " characters");
        }
        return bytes;
    }

    // A 32-byte base64 key is 44 characters, so it never looks like a typed key.
    private static byte[] decodeBase64Key(String key) {
        if (key == null || key.length() != BASE64_KEY_LENGTH) {
            return null;
        }
        try {
            byte[] decoded = Base64.getDecoder().decode(key);
            return decoded.length == KEY_SIZE_BYTES ? decoded : null;
        } catch (IllegalArgumentException notBase64) {
            return null;
        }
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
            throw new EncryptionException("Unable to decrypt value: it is empty");
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
