package com.amex.lumi.ingestion.dto;

import com.amex.lumi.ingestion.model.FileType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * Body of POST /api/v1/ingestions. Paths can be absolute or relative to the configured roots,
 * e.g. "samples/employees.csv" and "employees.properties".
 */
public record IngestionRequest(
        @NotBlank(message = "fileLocation is required")
        @Size(max = 1000, message = "fileLocation must not exceed 1000 characters")
        String fileLocation,

        @NotBlank(message = "controlFileLocation is required")
        @Size(max = 1000, message = "controlFileLocation must not exceed 1000 characters")
        String controlFileLocation,

        @NotNull(message = "fileType is required (CSV or JSON)")
        FileType fileType) {
}
