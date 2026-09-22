package com.amex.lumi.ingestion.dto;

import java.util.UUID;

/**
 * Returned with 202 Accepted: the ingestion runs in Airflow after this response.
 */
public record IngestionResponse(
        UUID executionId,
        String dagRunId,
        boolean requiresSplit,
        String status,
        String message) {
}
