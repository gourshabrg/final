package com.amex.lumi.ingestion.dto;

import java.time.Instant;
import java.util.UUID;

/**
 * Result of GET /api/v1/ingestions/{executionId}.
 */
public record IngestionStatusResponse(
        UUID executionId,
        String sourceFile,
        String status,
        Long expectedRecordCount,
        Long actualLoadedRecordCount,
        long errorRecordCount,
        Instant startedAt,
        Instant completedAt,
        String failureReason) {
}
