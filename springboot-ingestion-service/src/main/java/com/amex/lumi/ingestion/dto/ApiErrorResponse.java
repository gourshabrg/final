package com.amex.lumi.ingestion.dto;

import java.time.Instant;

/**
 * Error body returned by every endpoint.
 */
public record ApiErrorResponse(
        Instant timestamp,
        int status,
        String error,
        String message,
        String path) {
}
