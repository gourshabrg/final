package com.amex.lumi.beam.encryption;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class AesGcmEncryptionServiceTest {

    private AesGcmEncryptionService encryptionService;

    @BeforeEach
    void setUp() {

        byte[] key =
                "12345678901234567890123456789012"
                        .getBytes(
                                StandardCharsets.UTF_8
                        );

        encryptionService =
                new AesGcmEncryptionService(key);
    }


    @Test
    void shouldEncryptAndDecryptSuccessfully() {

        String original =
                "9876500000";

        String encrypted =
                encryptionService.encrypt(original);

        String decrypted =
                encryptionService.decrypt(encrypted);

        assertNotNull(encrypted);

        assertNotEquals(
                original,
                encrypted
        );

        assertEquals(
                original,
                decrypted
        );
    }


    @Test
    void shouldGenerateDifferentCiphertextForSamePlaintext() {

        String original =
                "9876500000";

        String encryptedFirst =
                encryptionService.encrypt(original);

        String encryptedSecond =
                encryptionService.encrypt(original);

        assertNotEquals(
                encryptedFirst,
                encryptedSecond
        );

        assertEquals(
                original,
                encryptionService.decrypt(
                        encryptedFirst
                )
        );

        assertEquals(
                original,
                encryptionService.decrypt(
                        encryptedSecond
                )
        );
    }


    @Test
    void shouldFailWhenCiphertextIsTampered() {

        String encrypted =
                encryptionService.encrypt(
                        "9876500000"
                );

        String[] parts =
                encrypted.split(
                        ":",
                        -1
                );

        String tamperedCiphertext =
                parts[0]
                        + ":"
                        + parts[1]
                        + ":"
                        + parts[2]
                                .substring(
                                        0,
                                        parts[2].length() - 1
                                )
                        + "A";

        assertThrows(
                EncryptionException.class,
                () -> encryptionService.decrypt(
                        tamperedCiphertext
                )
        );
    }


    @Test
    void shouldRejectInvalidKeyLength() {

        byte[] invalidKey =
                "short-key"
                        .getBytes(
                                StandardCharsets.UTF_8
                        );

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new AesGcmEncryptionService(
                                invalidKey
                        )
        );
    }
}
