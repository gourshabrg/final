package com.amex.lumi.ingestion.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * lumi.airflow.* settings: where Airflow runs and which DAG to trigger.
 */
@Validated
@ConfigurationProperties(prefix = "lumi.airflow")
public record AirflowProperties(
        @NotBlank String baseUrl,
        @NotBlank String username,
        @NotBlank String password,
        @NotBlank String dagId,
        @Positive int connectTimeoutSeconds,
        @Positive int readTimeoutSeconds) {
}
