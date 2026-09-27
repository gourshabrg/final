package com.amex.lumi.ingestion.security;

import com.amex.lumi.ingestion.config.EncryptionProperties;
import com.amex.lumi.ingestion.exception.DecryptionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;

/**
 * Decrypts values saved by the Beam job. Format: {@code v1:<iv>:<encrypted value>}, AES-256-GCM, same key.
 */
@Component
public class FieldDecryptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldDecryptor.class);

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String VERSION = "v1";
    private static final int KEY_SIZE_BYTES = 32;
    private static final int BASE64_KEY_LENGTH = 44;
    private static final int IV_SIZE_BYTES = 12;
    private static final int TAG_LENGTH_BITS = 128;

    private final SecretKeySpec key;

    public FieldDecryptor(EncryptionProperties properties) {
        this.key = new SecretKeySpec(keyBytes(properties.key()), "AES");
        LOGGER.info("Field decryption is ready");
    }

    // Same key rules as Beam, checked at startup.
    private static byte[] keyBytes(String key) {
        byte[] decoded = decodeBase64Key(key);
        if (decoded != null) {
            return decoded;
        }
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        if (bytes.length != KEY_SIZE_BYTES) {
            LOGGER.error("lumi.encryption.key has {} characters, expected 32 or a 32-byte base64 key", bytes.length);
            throw new IllegalStateException("lumi.encryption.key must be 32 bytes in base64 or exactly "
                    + KEY_SIZE_BYTES + " characters, but is " + bytes.length + " characters");
        }
        LOGGER.warn("lumi.encryption.key is 32 typed characters; a random key is stronger (openssl rand -base64 32)");
        return bytes;
    }

    // A 32-byte base64 key is 44 characters, so it never looks like a typed key.
    private static byte[] decodeBase64Key(String key) {
        if (key.length() != BASE64_KEY_LENGTH) {
            return null;
        }
        try {
            byte[] decoded = Base64.getDecoder().decode(key);
            return decoded.length == KEY_SIZE_BYTES ? decoded : null;
        } catch (IllegalArgumentException notBase64) {
            return null;
        }
    }

    public String decrypt(String encryptedValue) {
        try {
            String[] parts = encryptedValue.split(":", -1);
            if (parts.length != 3 || !VERSION.equals(parts[0])) {
                throw new IllegalArgumentException("not in v1:<iv>:<ciphertext> format");
            }
            byte[] iv = Base64.getDecoder().decode(parts[1]);
            if (iv.length != IV_SIZE_BYTES) {
                throw new IllegalArgumentException("invalid IV length");
            }
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_LENGTH_BITS, iv));
            return new String(cipher.doFinal(Base64.getDecoder().decode(parts[2])), StandardCharsets.UTF_8);
        } catch (GeneralSecurityException | IllegalArgumentException exception) {
            LOGGER.warn("Decryption failed: {}", exception.getMessage());
            throw new DecryptionException("Value could not be decrypted (wrong format or wrong key)", exception);
        }
    }

    /** Null or blank values were never encrypted, so they are returned as they are. */
    public String decryptIfPresent(String encryptedValue) {
        return encryptedValue == null || encryptedValue.isBlank() ? encryptedValue : decrypt(encryptedValue);
    }
}
