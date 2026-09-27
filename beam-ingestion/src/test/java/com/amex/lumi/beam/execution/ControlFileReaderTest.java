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
    void readsOptionalFileNameAndChecksum() throws IOException {
        String sha = "A".repeat(64);
        IngestionControl control = reader.read(controlFile(
                "record_count=2\nfile_name=employees.csv\nsha256=" + sha + "\n"));

        assertEquals("employees.csv", control.fileName());
        assertEquals("a".repeat(64), control.sha256());
    }

    @Test
    void rejectsChecksumInTheWrongFormat() throws IOException {
        String file = controlFile("record_count=2\nsha256=abc\n");
        assertThrows(ControlFileException.class, () -> reader.read(file));
    }

    @Test
    void dataFileMustMatchTheControlFile() throws IOException {
        Path data = Files.writeString(tempDir.resolve("employees.csv"), "employee_id\nEMP0001\n");
        String sha = SourceFileCheck.sha256(data);

        SourceFileCheck.verify(new IngestionControl(1, "employees.csv", sha), data, sha, 1L);
        ControlFileException wrongName = assertThrows(ControlFileException.class,
                () -> SourceFileCheck.verify(new IngestionControl(1, "other.csv", null), data, null, null));
        assertEquals("Control file is for other.csv but the data file is employees.csv", wrongName.getMessage());
        assertThrows(ControlFileException.class,
                () -> SourceFileCheck.verify(new IngestionControl(1, null, "0".repeat(64)), data, null, null));
    }

    @Test
    void filesChangedAfterTheRequestAreDetected() throws IOException {
        Path data = Files.writeString(tempDir.resolve("employees.csv"), "employee_id\nEMP0001\n");
        String shaAtRequest = SourceFileCheck.sha256(data);
        Files.writeString(data, "employee_id\nEMP0001\nEMP0002\n");

        ControlFileException changedData = assertThrows(ControlFileException.class,
                () -> SourceFileCheck.verify(new IngestionControl(2), data, shaAtRequest, null));
        assertEquals("Data file changed after the request (its sha256 is different)", changedData.getMessage());
        ControlFileException changedControl = assertThrows(ControlFileException.class,
                () -> SourceFileCheck.verify(new IngestionControl(2), data, null, 1L));
        assertEquals("Control file changed after the request: record_count is 2 but was 1",
                changedControl.getMessage());
    }
}
