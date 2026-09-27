package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.exception.InvalidRequestException;
import com.amex.lumi.ingestion.exception.ServerDataException;
import com.amex.lumi.ingestion.model.FileType;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Checks the data file before Airflow runs; messages show the client's path only.
 */
@Component
public class SourceFileValidator {

    /** @return file size in bytes */
    public long validate(Path file, String location, FileType fileType) {
        requireReadableFile(file, "Data file", location);
        String name = file.getFileName().toString().toLowerCase(Locale.ROOT);
        if (!name.endsWith(fileType.extension())) {
            throw new InvalidRequestException("Data file must end with " + fileType.extension()
                    + " for fileType " + fileType + ": " + location);
        }
        long size = sizeOf(file, location);
        if (size == 0) {
            throw new InvalidRequestException("Data file is empty: " + location);
        }
        return size;
    }

    static void requireReadableFile(Path file, String label, String location) {
        if (!Files.isRegularFile(file)) {
            throw new InvalidRequestException(label + " does not exist: " + location);
        }
        if (!Files.isReadable(file)) {
            throw new InvalidRequestException(label + " is not readable: " + location);
        }
    }

    private static long sizeOf(Path file, String location) {
        try {
            return Files.size(file);
        } catch (IOException exception) {
            // The file exists (checked above), so this is a disk problem on our side.
            throw new ServerDataException("Unable to read size of " + location, exception);
        }
    }
}
