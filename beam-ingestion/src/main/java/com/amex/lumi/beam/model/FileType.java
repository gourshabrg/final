package com.amex.lumi.beam.model;

import java.util.Locale;

/**
 * Supported source file formats.
 */
public enum FileType {

    CSV,
    JSON;

    /** Reads --fileType; "csv" and "CSV" both work. */
    public static FileType from(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("fileType is required");
        }
        try {
            return FileType.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Unsupported fileType '" + value + "'. Supported values: CSV, JSON");
        }
    }
}
