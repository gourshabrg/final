package com.amex.lumi.ingestion.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DecryptionServiceTest {

    @Test
    void decryptsEncryptedValueUsingConfiguredKey() {
        String encryptionKey = "0123456789abcdef0123456789abcdef";
        DecryptionService decryptionService = new DecryptionService(encryptionKey);

        String originalValue = "9876543210";
        String encryptedValue = decryptionService.encrypt(originalValue);

        assertEquals(originalValue, decryptionService.decrypt(encryptedValue));
    }

    @Test
    void rejectsBlankEncryptedValue() {
        DecryptionService decryptionService = new DecryptionService("0123456789abcdef0123456789abcdef");

        assertThrows(IllegalArgumentException.class, () -> decryptionService.decrypt("   "));
    }
}
