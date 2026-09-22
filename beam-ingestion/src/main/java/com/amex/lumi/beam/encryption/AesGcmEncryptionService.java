package com.amex.lumi.beam.encryption;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * AES-256-GCM implementation for authenticated field-level encryption.
 */
public class AesGcmEncryptionService
        implements EncryptionService {

    private static final String ALGORITHM = "AES";

    private static final String TRANSFORMATION =
            "AES/GCM/NoPadding";

    private static final String VERSION = "v1";

    private static final int KEY_SIZE_BYTES = 32;

    private static final int IV_SIZE_BYTES = 12;

    private static final int GCM_TAG_LENGTH_BITS = 128;

    private final SecretKeySpec secretKey;

    private final SecureRandom secureRandom;


    public AesGcmEncryptionService(byte[] key) {

        validateKey(key);

        this.secretKey =
                new SecretKeySpec(key, ALGORITHM);

        this.secureRandom =
                new SecureRandom();
    }


    @Override
    public String encrypt(String plainText) {

        if (plainText == null) {
            throw new IllegalArgumentException(
                    "plainText must not be null"
            );
        }

        try {

            byte[] iv =
                    new byte[IV_SIZE_BYTES];

            secureRandom.nextBytes(iv);

            Cipher cipher =
                    Cipher.getInstance(
                            TRANSFORMATION
                    );

            GCMParameterSpec gcmParameterSpec =
                    new GCMParameterSpec(
                            GCM_TAG_LENGTH_BITS,
                            iv
                    );

            cipher.init(
                    Cipher.ENCRYPT_MODE,
                    secretKey,
                    gcmParameterSpec
            );

            byte[] encryptedBytes =
                    cipher.doFinal(
                            plainText.getBytes(
                                    StandardCharsets.UTF_8
                            )
                    );

            return VERSION
                    + ":"
                    + Base64.getEncoder()
                            .encodeToString(iv)
                    + ":"
                    + Base64.getEncoder()
                            .encodeToString(encryptedBytes);

        } catch (GeneralSecurityException exception) {

            throw new EncryptionException(
                    "Unable to encrypt value",
                    exception
            );
        }
    }


    @Override
    public String decrypt(String encryptedText) {

        if (encryptedText == null
                || encryptedText.isBlank()) {

            throw new IllegalArgumentException(
                    "encryptedText must not be null or blank"
            );
        }

        try {

            String[] parts =
                    encryptedText.split(
                            ":",
                            -1
                    );

            if (parts.length != 3) {

                throw new IllegalArgumentException(
                        "Invalid encrypted value format"
                );
            }

            String version = parts[0];

            if (!VERSION.equals(version)) {

                throw new IllegalArgumentException(
                        "Unsupported encryption version: "
                                + version
                );
            }

            byte[] iv =
                    Base64.getDecoder()
                            .decode(parts[1]);

            byte[] encryptedBytes =
                    Base64.getDecoder()
                            .decode(parts[2]);

            if (iv.length != IV_SIZE_BYTES) {

                throw new IllegalArgumentException(
                        "Invalid IV length"
                );
            }

            Cipher cipher =
                    Cipher.getInstance(
                            TRANSFORMATION
                    );

            GCMParameterSpec gcmParameterSpec =
                    new GCMParameterSpec(
                            GCM_TAG_LENGTH_BITS,
                            iv
                    );

            cipher.init(
                    Cipher.DECRYPT_MODE,
                    secretKey,
                    gcmParameterSpec
            );

            byte[] plainTextBytes =
                    cipher.doFinal(
                            encryptedBytes
                    );

            return new String(
                    plainTextBytes,
                    StandardCharsets.UTF_8
            );

        } catch (GeneralSecurityException
                 | IllegalArgumentException exception) {

            throw new EncryptionException(
                    "Unable to decrypt value",
                    exception
            );
        }
    }


    private void validateKey(byte[] key) {

        if (key == null) {

            throw new IllegalArgumentException(
                    "Encryption key must not be null"
            );
        }

        if (key.length != KEY_SIZE_BYTES) {

            throw new IllegalArgumentException(
                    "AES-256 key must be exactly "
                            + KEY_SIZE_BYTES
                            + " bytes"
            );
        }
    }
}
