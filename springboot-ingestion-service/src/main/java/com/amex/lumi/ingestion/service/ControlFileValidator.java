package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.exception.InvalidRequestException;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Checks the control file has a valid record_count before the run starts.
 */
@Component
public class ControlFileValidator {

    private static final String RECORD_COUNT = "record_count";

    /** @return the expected record count */
    public long validate(Path controlFile) {
        SourceFileValidator.requireReadableFile(controlFile, "Control file");

        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(controlFile, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException exception) {
            throw new InvalidRequestException("Unable to read control file: " + controlFile);
        }

        String value = properties.getProperty(RECORD_COUNT);
        if (value == null || value.isBlank()) {
            throw new InvalidRequestException("Control file is missing " + RECORD_COUNT + ": " + controlFile);
        }
        try {
            long count = Long.parseLong(value.trim());
            if (count < 0) {
                throw new InvalidRequestException(RECORD_COUNT + " must not be negative");
            }
            return count;
        } catch (NumberFormatException exception) {
            throw new InvalidRequestException(RECORD_COUNT + " must be a whole number but was: " + value);
        }
    }
}
