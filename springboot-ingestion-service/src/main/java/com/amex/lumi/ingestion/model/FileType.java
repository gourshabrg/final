package com.amex.lumi.ingestion.model;

/**
 * Supported input formats and their file extensions.
 */
public enum FileType {

    CSV(".csv"),
    JSON(".json");

    private final String extension;

    FileType(String extension) {
        this.extension = extension;
    }

    public String extension() {
        return extension;
    }
}
