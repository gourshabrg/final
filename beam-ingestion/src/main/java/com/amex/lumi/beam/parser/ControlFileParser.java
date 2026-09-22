package com.amex.lumi.beam.parser;

import com.amex.lumi.beam.model.IngestionControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Reads and validates the metadata required to execute an ingestion.
 *
 * <p>The control file currently requires a non-negative {@code record_count}
 * property. Invalid or unreadable files fail before the Beam graph is built.
 */
public class ControlFileParser {

        private static final Logger LOGGER =
                        LoggerFactory.getLogger(ControlFileParser.class);

    private static final String RECORD_COUNT_PROPERTY =
            "record_count";

        /**
         * Parses an ingestion control file from the local filesystem.
         *
         * @param controlFileLocation path to the control file
         * @return validated ingestion metadata
         * @throws IllegalArgumentException when the file or its record count is invalid
         */
        public IngestionControl parse(
            String controlFileLocation) {

        if (controlFileLocation == null
                || controlFileLocation.isBlank()) {

            throw new IllegalArgumentException(
                    "Control file location must not be blank"
            );
        }

        Path controlFile =
                Path.of(controlFileLocation);

        if (!Files.exists(controlFile)) {

            throw new IllegalArgumentException(
                    "Control file does not exist: "
                            + controlFileLocation
            );
        }

        if (!Files.isRegularFile(controlFile)) {

            throw new IllegalArgumentException(
                    "Control file is not a regular file: "
                            + controlFileLocation
            );
        }

        if (!Files.isReadable(controlFile)) {

            throw new IllegalArgumentException(
                    "Control file is not readable: "
                            + controlFileLocation
            );
        }

        Properties properties =
                new Properties();

        try (InputStream inputStream =
                     Files.newInputStream(controlFile)) {

            properties.load(inputStream);

        } catch (IOException exception) {

                        LOGGER.error(
                                        "Unable to read control file: {}",
                                        controlFileLocation,
                                        exception
                        );

            throw new IllegalArgumentException(
                    "Unable to read control file: "
                            + controlFileLocation,
                    exception
            );
        }

        String recordCountValue =
                properties.getProperty(
                        RECORD_COUNT_PROPERTY
                );

        if (recordCountValue == null
                || recordCountValue.isBlank()) {

            throw new IllegalArgumentException(
                    "Control file is missing required property: "
                            + RECORD_COUNT_PROPERTY
            );
        }

        long expectedRecordCount;

        try {

            expectedRecordCount =
                    Long.parseLong(
                            recordCountValue.trim()
                    );

        } catch (NumberFormatException exception) {

                        LOGGER.error(
                                        "Invalid record_count in control file: {}",
                                        controlFileLocation,
                                        exception
                        );

            throw new IllegalArgumentException(
                    "Invalid record_count in control file: "
                            + recordCountValue,
                    exception
            );
        }

        if (expectedRecordCount < 0) {

            throw new IllegalArgumentException(
                    "record_count must not be negative: "
                            + expectedRecordCount
            );
        }

        return new IngestionControl(
                expectedRecordCount
        );
    }
}
