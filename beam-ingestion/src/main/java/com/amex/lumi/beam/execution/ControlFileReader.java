package com.amex.lumi.beam.execution;

import com.amex.lumi.beam.model.IngestionControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Reads the control file: record_count (required), file_name and sha256 (optional).
 */
public class ControlFileReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControlFileReader.class);

    public static final String RECORD_COUNT = "record_count";
    public static final String FILE_NAME = "file_name";
    public static final String SHA256 = "sha256";
    private static final Pattern SHA256_HEX = Pattern.compile("[0-9a-f]{64}");

    public IngestionControl read(String location) {
        if (location == null || location.isBlank()) {
            throw new ControlFileException("Control file location must not be blank");
        }
        Path path = Path.of(location);
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new ControlFileException("Control file does not exist or cannot be read: " + location);
        }

        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException exception) {
            throw new ControlFileException("Unable to read control file: " + location, exception);
        }

        IngestionControl control = new IngestionControl(recordCount(properties),
                optional(properties, FILE_NAME), sha256(properties));
        LOGGER.debug("Read {} from {}", control, location);
        return control;
    }

    private static long recordCount(Properties properties) {
        String value = properties.getProperty(RECORD_COUNT);
        if (value == null || value.isBlank()) {
            throw new ControlFileException("Control file is missing required property: " + RECORD_COUNT);
        }
        long recordCount;
        try {
            recordCount = Long.parseLong(value.trim());
        } catch (NumberFormatException exception) {
            throw new ControlFileException("record_count must be a whole number but was: " + value);
        }
        if (recordCount < 0) {
            throw new ControlFileException("record_count must not be negative: " + recordCount);
        }
        return recordCount;
    }

    private static String sha256(Properties properties) {
        String value = optional(properties, SHA256);
        if (value == null) {
            return null;
        }
        String hex = value.toLowerCase(Locale.ROOT);
        if (!SHA256_HEX.matcher(hex).matches()) {
            throw new ControlFileException("sha256 must be 64 hexadecimal characters");
        }
        return hex;
    }

    private static String optional(Properties properties, String key) {
        String value = properties.getProperty(key);
        return value == null || value.isBlank() ? null : value.trim();
    }
}
