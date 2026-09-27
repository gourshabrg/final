package com.amex.lumi.beam.read;

import com.amex.lumi.beam.TestEmployees;
import com.amex.lumi.beam.model.FileType;
import com.amex.lumi.beam.model.RecordFailure.FailureType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

class ReadEmployeeFilesTest {

    @TempDir
    Path tempDir;

    @Test
    void numbersRecordsAndSeparatesParseErrors() throws Exception {
        Path csv = tempDir.resolve("employees.csv");
        // Starts with a BOM, like files saved by Excel on Windows.
        Files.writeString(csv, "\uFEFF" + TestEmployees.CSV_HEADER + "\n"
                + TestEmployees.CSV_ROW + "\n"
                + TestEmployees.CSV_ROW.replace(",true,", ",maybe,") + "\n", StandardCharsets.UTF_8);

        Pipeline pipeline = Pipeline.create();
        PCollectionTuple result = pipeline.apply(
                new ReadEmployeeFiles(csv.toString(), FileType.CSV, "11111111-1111-1111-1111-111111111111"));

        PAssert.that(result.get(ReadEmployeeFiles.PARSED)
                        .apply("ParsedKeys", MapElements.into(TypeDescriptors.strings())
                                .via(parsed -> parsed.recordNumber() + ":" + parsed.employee().getEmployeeId())))
                .containsInAnyOrder("1:EMP0001");
        PAssert.that(result.get(ReadEmployeeFiles.PARSE_FAILURES)
                        .apply("FailureKeys", MapElements.into(TypeDescriptors.strings())
                                .via(failure -> failure.recordNumber() + ":" + failure.type())))
                .containsInAnyOrder("2:" + FailureType.PARSE_ERROR);

        pipeline.run().waitUntilFinish();
    }

    @Test
    void splitFilesAreReportedAgainstTheOriginalFile() throws Exception {
        Path splitFolder = Files.createDirectories(tempDir.resolve("split"));
        String header = TestEmployees.CSV_HEADER + ",source_record_number\n";
        Path part0 = Files.writeString(splitFolder.resolve("part-00000.csv"),
                header + TestEmployees.CSV_ROW.replace("EMP0001", "EMP0087") + ",87\n");
        Path part1 = Files.writeString(splitFolder.resolve("part-00001.csv"),
                header + TestEmployees.CSV_ROW.replace(",true,", ",maybe,") + ",12\n");
        String original = tempDir.resolve("employees_large.csv").toString();

        Pipeline pipeline = Pipeline.create();
        PCollectionTuple result = pipeline.apply(new ReadEmployeeFiles(splitFolder + "/*.csv", original,
                FileType.CSV, "11111111-1111-1111-1111-111111111111"));

        PAssert.that(result.get(ReadEmployeeFiles.PARSED)
                        .apply("ParsedLocations", MapElements.into(TypeDescriptors.strings())
                                .via(parsed -> parsed.recordNumber() + "|" + parsed.sourceFile() + "|"
                                        + parsed.splitFile())))
                .containsInAnyOrder("87|" + original + "|" + part0);
        PAssert.that(result.get(ReadEmployeeFiles.PARSE_FAILURES)
                        .apply("FailureLocations", MapElements.into(TypeDescriptors.strings())
                                .via(failure -> failure.recordNumber() + "|" + failure.sourceFile() + "|"
                                        + failure.splitFile())))
                .containsInAnyOrder("12|" + original + "|" + part1);

        pipeline.run().waitUntilFinish();
    }

    @Test
    void fileThatWasNotSplitHasNoSplitFile() throws Exception {
        Path csv = Files.writeString(tempDir.resolve("employees.csv"),
                TestEmployees.CSV_HEADER + "\n" + TestEmployees.CSV_ROW + "\n");

        Pipeline pipeline = Pipeline.create();
        PCollectionTuple result = pipeline.apply(new ReadEmployeeFiles(csv.toString(), csv.toString(),
                FileType.CSV, "11111111-1111-1111-1111-111111111111"));

        PAssert.that(result.get(ReadEmployeeFiles.PARSED)
                        .apply("Locations", MapElements.into(TypeDescriptors.strings())
                                .via(parsed -> parsed.recordNumber() + "|" + parsed.sourceFile() + "|"
                                        + parsed.splitFile())))
                .containsInAnyOrder("1|" + csv + "|null");

        pipeline.run().waitUntilFinish();
    }
}
