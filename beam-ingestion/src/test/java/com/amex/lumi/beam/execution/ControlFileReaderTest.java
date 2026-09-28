package com.amex.lumi.beam.execution;

import com.amex.lumi.beam.model.IngestionControl;
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
        assertThrows(ControlFileException.class, () -> reader.read(file));
    }

    @Test
    void rejectsNonNumericAndNegativeCounts() throws IOException {
        String text = controlFile("record_count=ten");
        assertThrows(ControlFileException.class, () -> reader.read(text));
        String negative = controlFile("record_count=-1");
        assertThrows(ControlFileException.class, () -> reader.read(negative));
    }

    @Test
    void rejectsMissingFile() {
        assertThrows(ControlFileException.class, () -> reader.read(tempDir.resolve("nope").toString()));
    }

    @Test
    void spacesAroundTheNumberAreAllowed() throws IOException {
        assertEquals(15, reader.read(controlFile("record_count =  15  \n")).expectedRecordCount());
    }

    @Test
    void blankLocationIsRejected() {
        assertThrows(ControlFileException.class, () -> reader.read(" "));
    }

    @Test
    void controlFileChangedAfterTheRequestIsDetected() {
        ControlFileReader.checkUnchanged(new IngestionControl(2), 2L);
        ControlFileReader.checkUnchanged(new IngestionControl(2), null);

        ControlFileException changedControl = assertThrows(ControlFileException.class,
                () -> ControlFileReader.checkUnchanged(new IngestionControl(2), 1L));
        assertEquals("Control file changed after the request: record_count is 2 but was 1",
                changedControl.getMessage());
    }
}
