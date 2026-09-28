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
 * Reads the control file, which holds only record_count.
 */
public class ControlFileReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControlFileReader.class);

    public static final String RECORD_COUNT = "record_count";

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

        IngestionControl control = new IngestionControl(recordCount(properties));
        LOGGER.debug("Read {} from {}", control, location);
        return control;
    }

    /** Fails if record_count differs from what the API read (null skips the check). */
    public static void checkUnchanged(IngestionControl control, Long countSeenByApi) {
        if (countSeenByApi != null && countSeenByApi != control.expectedRecordCount()) {
            throw new ControlFileException("Control file changed after the request: record_count is "
                    + control.expectedRecordCount() + " but was " + countSeenByApi);
        }
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
}
