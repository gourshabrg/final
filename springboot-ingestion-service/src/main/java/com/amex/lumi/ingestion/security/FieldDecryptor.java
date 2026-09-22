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
    private static final int IV_SIZE_BYTES = 12;
    private static final int TAG_LENGTH_BITS = 128;

    private final SecretKeySpec key;

    public FieldDecryptor(EncryptionProperties properties) {
        byte[] bytes = properties.key().getBytes(StandardCharsets.UTF_8);
        // Fail at startup, not on the first request.
        if (bytes.length != KEY_SIZE_BYTES) {
            LOGGER.error("lumi.encryption.key has {} bytes, expected {}", bytes.length, KEY_SIZE_BYTES);
            throw new IllegalStateException("lumi.encryption.key must be exactly " + KEY_SIZE_BYTES
                    + " bytes but is " + bytes.length);
        }
        this.key = new SecretKeySpec(bytes, "AES");
        LOGGER.info("Field decryption is ready");
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
