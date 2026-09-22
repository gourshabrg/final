package com.amex.lumi.ingestion.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * lumi.encryption.key - must be the same 32-character key the Beam job uses.
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
