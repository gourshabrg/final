package com.amex.lumi.beam.encryption;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AesGcmEncryptionServiceTest {

    private final AesGcmEncryptionService service =
            AesGcmEncryptionService.fromKey("0123456789abcdef0123456789abcdef");

    @Test
    void encryptsAndDecrypts() {
        String encrypted = service.encrypt("9876543210");

        assertTrue(encrypted.startsWith("v1:"));
        assertEquals("9876543210", service.decrypt(encrypted));
    }

    @Test
    void sameValueGivesDifferentCiphertext() {
        assertNotEquals(service.encrypt("950000"), service.encrypt("950000"));
    }

    @Test
    void tamperedValueIsRejected() {
        String encrypted = service.encrypt("950000");
        String tampered = encrypted.substring(0, encrypted.length() - 2) + "AA";

        assertThrows(EncryptionException.class, () -> service.decrypt(tampered));
    }

    @Test
    void keyMustBeThirtyTwoBytes() {
        assertThrows(IllegalArgumentException.class, () -> AesGcmEncryptionService.fromKey("too-short"));
    }

    @Test
    void blankValueCannotBeDecrypted() {
        assertThrows(IllegalArgumentException.class, () -> service.decrypt(" "));
    }

    @Test
    void wrongFormatCannotBeDecrypted() {
        assertThrows(EncryptionException.class, () -> service.decrypt("not-encrypted"));
    }

    @Test
    void unknownVersionCannotBeDecrypted() {
        String encrypted = service.encrypt("950000");

        assertThrows(EncryptionException.class, () -> service.decrypt(encrypted.replace("v1:", "v2:")));
    }

    @Test
    void nullCannotBeEncrypted() {
        assertThrows(IllegalArgumentException.class, () -> service.encrypt(null));
    }
}
