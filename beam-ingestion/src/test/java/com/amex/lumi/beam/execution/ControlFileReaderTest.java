package com.amex.lumi.beam.execution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ControlFileReaderTest {

    @TempDir
    Path tempDir;

    private final ControlFileReader reader = new ControlFileReader();

    private String controlFile(String content) throws IOException {
        Path file = tempDir.resolve("employees.properties");
        Files.writeString(file, content);
        return file.toString();
    }

    @Test
    void readsRecordCount() throws IOException {
        assertEquals(200, reader.read(controlFile("record_count=200\n")).expectedRecordCount());
    }

    @Test
    void rejectsMissingRecordCount() throws IOException {
        String file = controlFile("other=1");
        assertThrows(IllegalArgumentException.class, () -> reader.read(file));
    }

    @Test
    void rejectsNonNumericAndNegativeCounts() throws IOException {
        String text = controlFile("record_count=ten");
        assertThrows(IllegalArgumentException.class, () -> reader.read(text));
        String negative = controlFile("record_count=-1");
        assertThrows(IllegalArgumentException.class, () -> reader.read(negative));
    }

    @Test
    void rejectsMissingFile() {
        assertThrows(IllegalArgumentException.class, () -> reader.read(tempDir.resolve("nope").toString()));
    }

    @Test
    void spacesAroundTheNumberAreAllowed() throws IOException {
        assertEquals(15, reader.read(controlFile("record_count =  15  \n")).expectedRecordCount());
    }

    @Test
    void blankLocationIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> reader.read(" "));
    }
}
