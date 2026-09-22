package com.amex.lumi.beam.pipeline;

import com.amex.lumi.beam.execution.RecordCountCheck;
import com.amex.lumi.beam.model.EncryptedEmployee;
import com.amex.lumi.beam.model.FileType;
import com.amex.lumi.beam.model.RecordFailure;
import com.amex.lumi.beam.options.IngestionPipelineOptions;
import com.amex.lumi.beam.read.ReadEmployeeFiles;
import com.amex.lumi.beam.transform.PrepareForWarehouse;
import com.amex.lumi.beam.validate.ValidateEmployees;
import com.amex.lumi.beam.write.DatabaseConfig;
import com.amex.lumi.beam.write.LoadEmployees;
import com.amex.lumi.beam.write.WriteErrorReport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;

/**
 * Connects the pipeline steps: read -> validate -> prepare -> load -> count check, plus the error report.
 */
final class IngestionGraph {

    private IngestionGraph() {
    }

    static void build(Pipeline pipeline, IngestionPipelineOptions options,
                      DatabaseConfig database, RecordCountCheck.RunContext run) {
        String executionId = options.getExecutionId();

        PCollectionTuple read = pipeline.apply("ReadEmployees", new ReadEmployeeFiles(
                options.getInputFile(), FileType.from(options.getFileType()), executionId));

        PCollectionTuple validated = read.get(ReadEmployeeFiles.PARSED)
                .apply("ValidateEmployees", new ValidateEmployees(executionId));

        PCollection<EncryptedEmployee> rows = validated.get(ValidateEmployees.VALID)
                .apply("PrepareForWarehouse", new PrepareForWarehouse(executionId, options.getEncryptionKey()));

        PCollectionTuple loaded = rows.apply("LoadEmployees", new LoadEmployees(database));

        // All three kinds of failures go to one error report.
        PCollectionList.of(read.get(ReadEmployeeFiles.PARSE_FAILURES))
                .and(validated.get(ValidateEmployees.INVALID))
                .and(loaded.get(LoadEmployees.FAILED))
                .apply("CombineFailures", Flatten.<RecordFailure>pCollections())
                .apply("WriteErrorReport", new WriteErrorReport(options.getErrorOutput(), database));

        loaded.get(LoadEmployees.LOADED).apply("RecordCountCheck", new RecordCountCheck(run, database));
    }
}
