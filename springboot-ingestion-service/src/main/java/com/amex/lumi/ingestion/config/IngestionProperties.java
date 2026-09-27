package com.amex.lumi.ingestion.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * lumi.ingestion.* settings: local roots and the same folders inside Airflow.
 */
@Validated
@ConfigurationProperties(prefix = "lumi.ingestion")
public record IngestionProperties(
        @PositiveOrZero long fileSizeThresholdBytes,
        @NotBlank String localDataRoot,
        @NotBlank String airflowDataRoot,
        @NotBlank String localControlFileRoot,
        @NotBlank String airflowControlFileRoot) {
}
