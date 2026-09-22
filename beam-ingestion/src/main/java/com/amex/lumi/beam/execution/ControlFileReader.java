package com.amex.lumi.beam.execution;

import com.amex.lumi.beam.model.IngestionControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Reads record_count from the control file, before any data is written.
 */
public class ControlFileReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControlFileReader.class);

    static final String RECORD_COUNT = "record_count";

    public IngestionControl read(String location) {
        if (location == null || location.isBlank()) {
            throw new IllegalArgumentException("Control file location must not be blank");
        }
        Path path = Path.of(location);
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException("Control file does not exist or cannot be read: " + location);
        }

        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException exception) {
            throw new IllegalArgumentException("Unable to read control file: " + location, exception);
        }

        String value = properties.getProperty(RECORD_COUNT);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Control file is missing required property: " + RECORD_COUNT);
        }
        long recordCount;
        try {
            recordCount = Long.parseLong(value.trim());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("record_count must be a whole number but was: " + value);
        }
        if (recordCount < 0) {
            throw new IllegalArgumentException("record_count must not be negative: " + recordCount);
        }
        LOGGER.debug("Read record_count={} from {}", recordCount, location);
        return new IngestionControl(recordCount);
    }
}
