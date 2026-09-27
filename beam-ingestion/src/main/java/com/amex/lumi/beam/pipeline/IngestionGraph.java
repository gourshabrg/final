package com.amex.lumi.beam.pipeline;

import com.amex.lumi.beam.execution.RecordCountCheck;
import com.amex.lumi.beam.model.EncryptedEmployee;
import com.amex.lumi.beam.model.FileType;
import com.amex.lumi.beam.model.RecordFailure;
import com.amex.lumi.beam.options.IngestionPipelineOptions;
import com.amex.lumi.beam.read.ReadEmployeeFiles;
import com.amex.lumi.beam.transform.PrepareForWarehouse;
import com.amex.lumi.beam.validate.RejectDuplicateIds;
import com.amex.lumi.beam.validate.ValidateEmployees;
import com.amex.lumi.beam.write.DatabaseConfig;
import com.amex.lumi.beam.write.LoadEmployees;
import com.amex.lumi.beam.write.WriteErrorReport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Instant;

/**
 * Pipeline steps: read, validate, reject duplicates, prepare, load, count check, error report.
 */
final class IngestionGraph {

    private IngestionGraph() {
    }

    static void build(Pipeline pipeline, IngestionPipelineOptions options,
                      DatabaseConfig database, RecordCountCheck.RunContext run, String encryptionKey) {
        String executionId = options.getExecutionId();

        PCollectionTuple read = pipeline.apply("ReadEmployees", new ReadEmployeeFiles(options.getInputFile(),
                EmployeeIngestionPipeline.sourceFileOf(options), FileType.from(options.getFileType()), executionId));

        PCollectionTuple validated = read.get(ReadEmployeeFiles.PARSED)
                .apply("ValidateEmployees", new ValidateEmployees(executionId));

        PCollectionTuple unique = validated.get(ValidateEmployees.VALID)
                .apply("RejectDuplicateIds", new RejectDuplicateIds(executionId));

        PCollection<EncryptedEmployee> rows = unique.get(RejectDuplicateIds.UNIQUE)
                .apply("PrepareForWarehouse", new PrepareForWarehouse(executionId, encryptionKey,
                        lastModified(EmployeeIngestionPipeline.sourceFileOf(options))));

        PCollectionTuple loaded = rows.apply("LoadEmployees", new LoadEmployees(database));

        // All kinds of failures go to one error report.
        PCollectionList.of(read.get(ReadEmployeeFiles.PARSE_FAILURES))
                .and(validated.get(ValidateEmployees.INVALID))
                .and(unique.get(RejectDuplicateIds.DUPLICATES))
                .and(loaded.get(LoadEmployees.FAILED))
                .apply("CombineFailures", Flatten.<RecordFailure>pCollections())
                .apply("WriteErrorReport", new WriteErrorReport(options.getErrorOutput(), database));

        loaded.get(LoadEmployees.LOADED).apply("RecordCountCheck", new RecordCountCheck(run, database));
    }

    // Null when the file cannot be read here; such rows always replace the stored ones.
    private static Instant lastModified(String file) {
        try {
            return Files.getLastModifiedTime(Path.of(file)).toInstant();
        } catch (IOException | InvalidPathException exception) {
            return null;
        }
    }
}
