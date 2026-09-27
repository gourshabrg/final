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

    private Path data() {
        return tempDir.resolve("employees.csv");
    }

    @Test
    void controlFileMustNameAndFingerprintTheDataFile() throws IOException {
        Path data = Files.writeString(data(), "employee_id\nEMP0001\n");
        String sha = FileChecksum.sha256(data);
        Path good = Files.writeString(tempDir.resolve("good.properties"),
                "record_count=1\nfile_name=employees.csv\nsha256=" + sha.toUpperCase() + "\n");
        Path wrongName = Files.writeString(tempDir.resolve("name.properties"), "record_count=1\nfile_name=x.csv\n");
        Path wrongSha = Files.writeString(tempDir.resolve("sha.properties"),
                "record_count=1\nsha256=" + "0".repeat(64));
        Path badSha = Files.writeString(tempDir.resolve("bad.properties"), "record_count=1\nsha256=abc\n");

        assertThat(controlValidator.validate(good, "good.properties", "employees.csv", sha)).isEqualTo(1);
        assertThatThrownBy(() -> controlValidator.validate(wrongName, "name.properties", "employees.csv", sha))
                .hasMessage("Control file is for x.csv but the data file is employees.csv");
        assertThatThrownBy(() -> controlValidator.validate(wrongSha, "sha.properties", "employees.csv", sha))
                .hasMessageContaining("does not match the sha256");
        assertThatThrownBy(() -> controlValidator.validate(badSha, "bad.properties", "employees.csv", sha))
                .hasMessageContaining("64 hexadecimal");
    }

    @Test
    void acceptsCsvAndReturnsSize() throws IOException {
        Path csv = Files.writeString(tempDir.resolve("employees.csv"), "header\n");

        assertThat(sourceValidator.validate(csv, "requested.file", FileType.CSV)).isEqualTo(7);
    }

    @Test
    void rejectsWrongExtensionMissingAndEmptyFiles() throws IOException {
        Path json = Files.writeString(tempDir.resolve("employees.json"), "[]");
        Path empty = Files.createFile(tempDir.resolve("empty.csv"));

        assertThatThrownBy(() -> sourceValidator.validate(json, "requested.file", FileType.CSV))
                .isInstanceOf(InvalidRequestException.class).hasMessageContaining("must end with .csv");
        assertThatThrownBy(() -> sourceValidator.validate(tempDir.resolve("nope.csv"), "nope.csv", FileType.CSV))
                .isInstanceOf(InvalidRequestException.class).hasMessageContaining("does not exist");
        assertThatThrownBy(() -> sourceValidator.validate(empty, "requested.file", FileType.CSV))
                .isInstanceOf(InvalidRequestException.class).hasMessageContaining("empty");
    }

    @Test
    void readsRecordCountFromControlFile() throws IOException {
        Path control = Files.writeString(tempDir.resolve("c.properties"), "record_count=20\n");

        assertThat(controlValidator.validate(control, "requested.properties", "employees.csv", null)).isEqualTo(20);
    }

    @Test
    void rejectsBadControlFiles() throws IOException {
        Path missing = Files.writeString(tempDir.resolve("a.properties"), "x=1");
        Path text = Files.writeString(tempDir.resolve("b.properties"), "record_count=ten");

        assertThatThrownBy(() -> controlValidator.validate(missing, "a.properties", "employees.csv", null))
                .hasMessageContaining("missing record_count");
        assertThatThrownBy(() -> controlValidator.validate(text, "b.properties", "employees.csv", null))
                .hasMessageContaining("whole number");
    }

    @Test
    void acceptsJsonFile() throws IOException {
        Path json = Files.writeString(tempDir.resolve("employees.json"), "[]");

        assertThat(sourceValidator.validate(json, "requested.file", FileType.JSON)).isEqualTo(2);
    }

    @Test
    void extensionCheckIgnoresLetterCase() throws IOException {
        Path csv = Files.writeString(tempDir.resolve("EMPLOYEES.CSV"), "header\n");

        assertThat(sourceValidator.validate(csv, "requested.file", FileType.CSV)).isPositive();
    }

    @Test
    void rejectsNegativeRecordCount() throws IOException {
        Path negative = Files.writeString(tempDir.resolve("n.properties"), "record_count=-5");

        assertThatThrownBy(() -> controlValidator.validate(negative, "n.properties", "employees.csv", null))
                .hasMessageContaining("must not be negative");
    }

    @Test
    void errorShowsTheRequestedPathNotTheServerPath() {
        Path missing = tempDir.resolve("nope.csv");

        assertThatThrownBy(() -> sourceValidator.validate(missing, "samples/nope.csv", FileType.CSV))
                .hasMessage("Data file does not exist: samples/nope.csv")
                .hasMessageNotContaining(tempDir.toString());
    }
}
