package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.exception.InvalidRequestException;
import com.amex.lumi.ingestion.model.FileType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileValidatorsTest {

    @TempDir
    Path tempDir;

    private final SourceFileValidator sourceValidator = new SourceFileValidator();
    private final ControlFileValidator controlValidator = new ControlFileValidator();

    @Test
    void acceptsCsvAndReturnsSize() throws IOException {
        Path csv = Files.writeString(tempDir.resolve("employees.csv"), "header\n");

        assertThat(sourceValidator.validate(csv, FileType.CSV)).isEqualTo(7);
    }

    @Test
    void rejectsWrongExtensionMissingAndEmptyFiles() throws IOException {
        Path json = Files.writeString(tempDir.resolve("employees.json"), "[]");
        Path empty = Files.createFile(tempDir.resolve("empty.csv"));

        assertThatThrownBy(() -> sourceValidator.validate(json, FileType.CSV))
                .isInstanceOf(InvalidRequestException.class).hasMessageContaining("must end with .csv");
        assertThatThrownBy(() -> sourceValidator.validate(tempDir.resolve("nope.csv"), FileType.CSV))
                .isInstanceOf(InvalidRequestException.class).hasMessageContaining("does not exist");
        assertThatThrownBy(() -> sourceValidator.validate(empty, FileType.CSV))
                .isInstanceOf(InvalidRequestException.class).hasMessageContaining("empty");
    }

    @Test
    void readsRecordCountFromControlFile() throws IOException {
        Path control = Files.writeString(tempDir.resolve("c.properties"), "record_count=20\n");

        assertThat(controlValidator.validate(control)).isEqualTo(20);
    }

    @Test
    void rejectsBadControlFiles() throws IOException {
        Path missing = Files.writeString(tempDir.resolve("a.properties"), "x=1");
        Path text = Files.writeString(tempDir.resolve("b.properties"), "record_count=ten");

        assertThatThrownBy(() -> controlValidator.validate(missing)).hasMessageContaining("missing record_count");
        assertThatThrownBy(() -> controlValidator.validate(text)).hasMessageContaining("whole number");
    }

    @Test
    void acceptsJsonFile() throws IOException {
        Path json = Files.writeString(tempDir.resolve("employees.json"), "[]");

        assertThat(sourceValidator.validate(json, FileType.JSON)).isEqualTo(2);
    }

    @Test
    void extensionCheckIgnoresLetterCase() throws IOException {
        Path csv = Files.writeString(tempDir.resolve("EMPLOYEES.CSV"), "header\n");

        assertThat(sourceValidator.validate(csv, FileType.CSV)).isPositive();
    }

    @Test
    void rejectsNegativeRecordCount() throws IOException {
        Path negative = Files.writeString(tempDir.resolve("n.properties"), "record_count=-5");

        assertThatThrownBy(() -> controlValidator.validate(negative)).hasMessageContaining("must not be negative");
    }
}
