package com.amex.lumi.ingestion.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * lumi.encryption.key - must be the same key the Beam job uses (32 bytes in base64, or 32 characters).
 */
@Validated
@ConfigurationProperties(prefix = "lumi.encryption")
public record EncryptionProperties(@NotBlank String key) {

    // Keep the key out of logs.
    @Override
    public String toString() {
        return "EncryptionProperties{key='****'}";
    }
}
