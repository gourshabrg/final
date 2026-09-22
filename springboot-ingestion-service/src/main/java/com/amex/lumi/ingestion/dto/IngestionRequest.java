package com.amex.lumi.ingestion.dto;

import com.amex.lumi.ingestion.enums.FileType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public class IngestionRequest {

    @NotBlank(message = "fileLocation is required")
    @Size(
            max = 1000,
            message = "fileLocation must not exceed 1000 characters"
    )
    private String fileLocation;

    @NotNull(message = "fileType is required")
    private FileType fileType;

    @NotBlank(message = "controlFileLocation is required")
    @Size(
            max = 1000,
            message = "controlFileLocation must not exceed 1000 characters"
    )
    private String controlFileLocation;

    public IngestionRequest() {
    }

    public String getFileLocation() {
        return fileLocation;
    }

    public void setFileLocation(String fileLocation) {
        this.fileLocation = fileLocation;
    }

    public FileType getFileType() {
        return fileType;
    }

    public void setFileType(FileType fileType) {
        this.fileType = fileType;
    }

    public String getControlFileLocation() {
        return controlFileLocation;
    }

    public void setControlFileLocation(String controlFileLocation) {
        this.controlFileLocation = controlFileLocation;
    }
}
