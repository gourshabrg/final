package com.amex.lumi.ingestion.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * Body of POST /api/v1/decrypt, e.g. {"encryptedValue": "v1:...:..."}.
 */
public record DecryptionRequest(@NotBlank(message = "encryptedValue is required") String encryptedValue) {
}
