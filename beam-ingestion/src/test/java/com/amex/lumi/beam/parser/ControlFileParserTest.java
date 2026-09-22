package com.amex.lumi.beam.parser;

import com.amex.lumi.beam.model.IngestionControl;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ControlFileParserTest {

    private final ControlFileParser parser =
            new ControlFileParser();

    @Test
    void shouldParseRecordCount() throws Exception {

        Path controlFile =
                Files.createTempFile(
                        "control-",
                        ".properties"
                );

        Files.writeString(
                controlFile,
                "record_count=25"
        );

        IngestionControl result =
                parser.parse(
                        controlFile.toString()
                );

        assertEquals(
                25,
                result.getExpectedRecordCount()
        );

        Files.deleteIfExists(controlFile);
    }

    @Test
    void shouldRejectMissingRecordCount() throws Exception {

        Path controlFile =
                Files.createTempFile(
                        "control-",
                        ".properties"
                );

        Files.writeString(
                controlFile,
                "some_other_property=value"
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> parser.parse(
                        controlFile.toString()
                )
        );

        Files.deleteIfExists(controlFile);
    }

    @Test
    void shouldRejectInvalidRecordCount() throws Exception {

        Path controlFile =
                Files.createTempFile(
                        "control-",
                        ".properties"
                );

        Files.writeString(
                controlFile,
                "record_count=abc"
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> parser.parse(
                        controlFile.toString()
                )
        );

        Files.deleteIfExists(controlFile);
    }

    @Test
    void shouldRejectNegativeRecordCount() throws Exception {

        Path controlFile =
                Files.createTempFile(
                        "control-",
                        ".properties"
                );

        Files.writeString(
                controlFile,
                "record_count=-1"
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> parser.parse(
                        controlFile.toString()
                )
        );

        Files.deleteIfExists(controlFile);
    }

    @Test
    void shouldRejectMissingControlFile() {

        assertThrows(
                IllegalArgumentException.class,
                () -> parser.parse(
                        "does-not-exist.properties"
                )
        );
    }
}
