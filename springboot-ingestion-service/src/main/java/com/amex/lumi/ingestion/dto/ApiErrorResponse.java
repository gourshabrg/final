package com.amex.lumi.ingestion.dto;

import java.time.Instant;

/**
 * Error body returned by every endpoint. "code" is stable for programs; "message" is for people.
 */
public record ApiErrorResponse(
        Instant timestamp,
        int status,
        String error,
        String code,
        String message,
        String path) {
}
