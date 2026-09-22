package com.amex.lumi.beam.execution;

import com.amex.lumi.beam.model.EncryptedEmployee;
import com.amex.lumi.beam.model.IngestionExecution;
import com.amex.lumi.beam.model.RecordCountCheckResult;
import com.amex.lumi.beam.write.DatabaseConfig;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;

/**
 * Phase 3: loaded count vs control file. Match -> SUCCESS. Mismatch -> save FAILED, then fail the run.
 */
public class RecordCountCheck extends PTransform<PCollection<EncryptedEmployee>, PDone> {

    private static final TupleTag<RecordCountCheckResult> MATCHED = new TupleTag<>() {
    };
    private static final TupleTag<RecordCountCheckResult> MISMATCHED = new TupleTag<>() {
    };

    private final RunContext run;
    private final DatabaseConfig database;

    public RecordCountCheck(RunContext run, DatabaseConfig database) {
        this.run = run;
        this.database = database;
    }

    @Override
    public PDone expand(PCollection<EncryptedEmployee> loaded) {
        PCollectionTuple results = loaded
                .apply("CountLoadedRows", Count.globally())
                .apply("CompareWithControlFile", ParDo.of(new CompareCountFn(run.expectedRecordCount()))
                        .withOutputTags(MATCHED, TupleTagList.of(MISMATCHED)));

        results.get(MATCHED)
                .apply("BuildSuccessStatus", ParDo.of(new ToExecutionFn(run)))
                .apply("SaveSuccessStatus", ParDo.of(new SaveExecutionFn(database)));

        PCollection<RecordCountCheckResult> mismatched = results.get(MISMATCHED);
        PCollection<Void> failureSaved = mismatched
                .apply("BuildFailedStatus", ParDo.of(new ToExecutionFn(run)))
                .apply("SaveFailedStatus", ParDo.of(new SaveExecutionFn(database)));

        // Throw only after FAILED is saved, otherwise the status could stay RUNNING.
        mismatched
                .apply("WaitForFailedStatus", Wait.on(failureSaved))
                .apply("FailRunOnMismatch", ParDo.of(new FailOnMismatchFn()));

        return PDone.in(loaded.getPipeline());
    }

    /** Details of the current run. */
    public record RunContext(String executionId, String sourceFile, long expectedRecordCount, Instant startedAt)
            implements java.io.Serializable {
    }

    static class CompareCountFn extends DoFn<Long, RecordCountCheckResult> {

        private static final Logger LOGGER = LoggerFactory.getLogger(CompareCountFn.class);
        private final long expected;

        CompareCountFn(long expected) {
            this.expected = expected;
        }

        @ProcessElement
        public void processElement(@Element Long actual, MultiOutputReceiver out) {
            RecordCountCheckResult result = new RecordCountCheckResult(expected, actual);
            if (result.matched()) {
                LOGGER.info("Record count check passed: expected={} loaded={}", expected, actual);
                out.get(MATCHED).output(result);
            } else {
                LOGGER.error(result.failureMessage());
                out.get(MISMATCHED).output(result);
            }
        }
    }

    static class ToExecutionFn extends DoFn<RecordCountCheckResult, IngestionExecution> {

        private final RunContext run;

        ToExecutionFn(RunContext run) {
            this.run = run;
        }

        @ProcessElement
        public void processElement(@Element RecordCountCheckResult result, OutputReceiver<IngestionExecution> out) {
            Instant now = Instant.now();
            out.output(result.matched()
                    ? IngestionExecution.succeeded(run.executionId(), run.sourceFile(), run.expectedRecordCount(),
                    result.actualLoadedRecordCount(), run.startedAt(), now)
                    : IngestionExecution.failed(run.executionId(), run.sourceFile(), run.expectedRecordCount(),
                    result.actualLoadedRecordCount(), run.startedAt(), now, result.failureMessage()));
        }
    }

    static class SaveExecutionFn extends DoFn<IngestionExecution, Void> {

        private final DatabaseConfig database;
        private transient ExecutionStatusRepository repository;

        SaveExecutionFn(DatabaseConfig database) {
            this.database = database;
        }

        @Setup
        public void setup() {
            repository = new ExecutionStatusRepository(database);
        }

        @ProcessElement
        public void processElement(@Element IngestionExecution execution) throws SQLException {
            repository.save(execution);
        }
    }

    static class FailOnMismatchFn extends DoFn<RecordCountCheckResult, Void> {
        @ProcessElement
        public void processElement(@Element RecordCountCheckResult result) {
            throw new RecordCountMismatchException(result.failureMessage());
        }
    }
}
