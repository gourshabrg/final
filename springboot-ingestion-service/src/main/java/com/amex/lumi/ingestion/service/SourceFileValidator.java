package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.exception.InvalidRequestException;
import com.amex.lumi.ingestion.model.FileType;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Checks the data file before we trigger Airflow, so bad requests fail fast with 400.
 */
@Component
public class SourceFileValidator {

    /** @return file size in bytes */
    public long validate(Path file, FileType fileType) {
        requireReadableFile(file, "Data file");
        String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
        if (!name.endsWith(fileType.extension())) {
            throw new InvalidRequestException("Data file must end with " + fileType.extension()
                    + " for fileType " + fileType + ": " + file.getFileName());
        }
        long size = sizeOf(file);
        if (size == 0) {
            throw new InvalidRequestException("Data file is empty: " + file);
        }
        return size;
    }

    static void requireReadableFile(Path file, String label) {
        if (!Files.isRegularFile(file)) {
            throw new InvalidRequestException(label + " does not exist: " + file);
        }
        if (!Files.isReadable(file)) {
            throw new InvalidRequestException(label + " is not readable: " + file);
        }
    }

    private static long sizeOf(Path file) {
        try {
            return Files.size(file);
        } catch (IOException exception) {
            throw new InvalidRequestException("Unable to read size of " + file);
        }
    }
}
