package com.amex.lumi.ingestion.dto;

/**
 * Result of POST /api/v1/decrypt.
 */
public record DecryptionResponse(String decryptedValue) {
}
