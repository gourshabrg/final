package com.amex.lumi.ingestion.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

@Service
public class DecryptionService {

    private static final String ALGORITHM = "AES";
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String VERSION = "v1";
    private static final int IV_SIZE_BYTES = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;

    private final SecretKeySpec secretKey;

    @Autowired
    public DecryptionService(@Value("${lumi.encryption.key}") String key) {
        this(getKeyBytes(key));
    }

    public DecryptionService(byte[] keyBytes) {
        if (keyBytes == null || keyBytes.length != 32) {
            throw new IllegalArgumentException("AES-256 key must be exactly 32 bytes");
        }
        this.secretKey = new SecretKeySpec(keyBytes, ALGORITHM);
    }

    public String encrypt(String plainText) {
        if (plainText == null) {
            throw new IllegalArgumentException("plainText must not be null");
        }

        try {
            byte[] iv = new byte[IV_SIZE_BYTES];
            new java.security.SecureRandom().nextBytes(iv);

            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, gcmParameterSpec);

            byte[] encryptedBytes = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));

            return VERSION
                    + ":"
                    + Base64.getEncoder().encodeToString(iv)
                    + ":"
                    + Base64.getEncoder().encodeToString(encryptedBytes);

        } catch (GeneralSecurityException exception) {
            throw new IllegalArgumentException("Unable to encrypt value", exception);
        }
    }

    public String decrypt(String encryptedText) {
        if (encryptedText == null || encryptedText.isBlank()) {
            throw new IllegalArgumentException("encryptedText must not be null or blank");
        }

        try {
            String[] parts = encryptedText.split(":", -1);
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid encrypted value format");
            }

            String version = parts[0];
            if (!VERSION.equals(version)) {
                throw new IllegalArgumentException("Unsupported encryption version: " + version);
            }

            byte[] iv = Base64.getDecoder().decode(parts[1]);
            byte[] encryptedBytes = Base64.getDecoder().decode(parts[2]);

            if (iv.length != IV_SIZE_BYTES) {
                throw new IllegalArgumentException("Invalid IV length");
            }

            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, gcmParameterSpec);

            byte[] plainTextBytes = cipher.doFinal(encryptedBytes);
            return new String(plainTextBytes, StandardCharsets.UTF_8);

        } catch (GeneralSecurityException | IllegalArgumentException exception) {
            throw new IllegalArgumentException("Unable to decrypt value", exception);
        }
    }

    private static byte[] getKeyBytes(String rawKey) {
        if (rawKey == null || rawKey.isBlank()) {
            throw new IllegalArgumentException("Encryption key must not be blank");
        }

        byte[] keyBytes = rawKey.getBytes(StandardCharsets.UTF_8);
        if (keyBytes.length == 32) {
            return keyBytes;
        }

        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            return sha256.digest(rawKey.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
