package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.exception.InvalidRequestException;
import com.amex.lumi.ingestion.exception.ServerDataException;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Checks record_count and, when given, that file_name and sha256 match the data file.
 */
@Component
public class ControlFileValidator {

    private static final String RECORD_COUNT = "record_count";
    private static final String FILE_NAME = "file_name";
    private static final String SHA256 = "sha256";
    private static final Pattern SHA256_HEX = Pattern.compile("[0-9a-f]{64}");

    /** @return the expected record count */
    public long validate(Path controlFile, String location, String dataFileName, String dataSha256) {
        SourceFileValidator.requireReadableFile(controlFile, "Control file", location);

        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(controlFile, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException exception) {
            throw new ServerDataException("Unable to read control file: " + location, exception);
        }

        long count = recordCount(properties, location);
        checkFileName(properties.getProperty(FILE_NAME), dataFileName);
        checkSha256(properties.getProperty(SHA256), dataSha256);
        return count;
    }

    private static long recordCount(Properties properties, String location) {
        String value = properties.getProperty(RECORD_COUNT);
        if (value == null || value.isBlank()) {
            throw new InvalidRequestException("Control file is missing " + RECORD_COUNT + ": " + location);
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

    private static void checkFileName(String expected, String actual) {
        if (expected != null && !expected.isBlank() && !expected.trim().equals(actual)) {
            throw new InvalidRequestException("Control file is for " + expected.trim()
                    + " but the data file is " + actual);
        }
    }

    private static void checkSha256(String expected, String actual) {
        if (expected == null || expected.isBlank()) {
            return;
        }
        String hex = expected.trim().toLowerCase(Locale.ROOT);
        if (!SHA256_HEX.matcher(hex).matches()) {
            throw new InvalidRequestException(SHA256 + " must be 64 hexadecimal characters");
        }
        if (!hex.equals(actual)) {
            throw new InvalidRequestException("Data file content does not match the sha256 in the control file");
        }
    }
}
