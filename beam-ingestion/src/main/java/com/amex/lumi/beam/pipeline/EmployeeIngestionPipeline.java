package com.amex.lumi.beam.pipeline;
import com.amex.lumi.beam.sink.IngestionExecutionRepository;
import org.apache.beam.sdk.transforms.DoFn;
import com.amex.lumi.beam.model.RecordCountCheckResult;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.transforms.Wait;
import com.amex.lumi.beam.model.EncryptedEmployeeRecord;
import com.amex.lumi.beam.model.IngestionControl;
import com.amex.lumi.beam.model.IngestionExecution;
import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.options.IngestionPipelineOptions;
import com.amex.lumi.beam.parser.ControlFileParser;
import com.amex.lumi.beam.parser.ReadCsvEmployees;
import com.amex.lumi.beam.sink.LoadFailure;
import com.amex.lumi.beam.sink.WriteEmployeeToPostgresFn;
import com.amex.lumi.beam.sink.WriteIngestionErrorToPostgresFn;
import com.amex.lumi.beam.sink.WriteIngestionExecutionToPostgresFn;
import com.amex.lumi.beam.transform.EnrichEmployeeMetadataFn;
import com.amex.lumi.beam.transform.EncryptEmployeeFieldsFn;
import com.amex.lumi.beam.transform.ErrorRecordFormatter;
import com.amex.lumi.beam.transform.IngestionExecutionFactory;
import com.amex.lumi.beam.transform.LoadFailureFormatter;
import com.amex.lumi.beam.transform.LoadedRecordCounter;
import com.amex.lumi.beam.transform.ReadJsonEmployees;
import com.amex.lumi.beam.transform.ValidationFailureToLoadFailureFn;
import com.amex.lumi.beam.validation.EmployeeValidationFn;
import com.amex.lumi.beam.validation.ValidationFailure;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.TimeZone;
import java.time.Instant;

public class EmployeeIngestionPipeline {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(
                    EmployeeIngestionPipeline.class
            );
            	private static final TupleTag<RecordCountCheckResult> MATCHED_COUNTS =
        new TupleTag<>() {};

private static final TupleTag<RecordCountCheckResult> MISMATCHED_COUNTS =
        new TupleTag<>() {};


    public static void main(String[] args) {

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));


        LOGGER.info(
                "Starting employee ingestion pipeline"
        );

        IngestionPipelineOptions options =
                PipelineOptionsFactory
                        .fromArgs(args)
                        .withValidation()
                        .as(IngestionPipelineOptions.class);

        ControlFileParser controlFileParser =
                new ControlFileParser();

        IngestionControl ingestionControl =
                controlFileParser.parse(
                        options.getControlFile()
                );

        long expectedRecordCount =
                ingestionControl.getExpectedRecordCount();

        LOGGER.info(
                "Expected record count from control file: {}",
                expectedRecordCount
        );

        

        validateOptions(options);

        IngestionExecutionFactory executionFactory =
                new IngestionExecutionFactory();

        Instant startedAt = Instant.now();

        /*
         * Create the execution record before Beam starts.
         *
         * This is deliberately done outside the Beam graph because
         * STARTED represents the lifecycle of the entire ingestion job.
         */
        IngestionExecution startedExecution =
                executionFactory.createStarted(
                        options.getExecutionId(),
                        options.getInputFile(),
                        expectedRecordCount,
                        startedAt
                );

        writeExecutionStatus(
                startedExecution,
                options
        );

        /*
         * Mark execution as RUNNING before submitting the Beam graph.
         */
        IngestionExecution runningExecution =
                executionFactory.createRunning(
                        options.getExecutionId(),
                        options.getInputFile(),
                        expectedRecordCount,
                        startedAt
                );

        writeExecutionStatus(
                runningExecution,
                options
        );

        Pipeline pipeline =
                Pipeline.create(options);

        LOGGER.info(
                "Input file: {}",
                options.getInputFile()
        );

        LOGGER.info(
                "Execution ID: {}",
                options.getExecutionId()
        );

        LOGGER.info(
                "Source creation time: {}",
                options.getSourceCreationTime()
        );

        LOGGER.info(
                "Error output: {}",
                options.getErrorOutput()
        );

        /*
         * ------------------------------------------------------------
         * 1. READ
         * ------------------------------------------------------------
         *
         * Current implementation supports JSON array input.
         */
        // PCollection<org.apache.beam.sdk.values.KV<
        //         Long,
        //         EmployeeRecord>> employees =
        //         pipeline.apply(
        //                 "ReadEmployeeJson",
        //                 new ReadJsonEmployees(
        //                         options.getInputFile()
        //                 )
        //         );

         /*
 * ------------------------------------------------------------
 * 1. READ
 * ------------------------------------------------------------
 *
 * Select the reader based on the input file type.
 *
 * JSON and CSV readers produce the same output:
 *
 * KV<Long, EmployeeRecord>
 *
 * Everything after this point is shared.
 */
PCollection<org.apache.beam.sdk.values.KV<
        Long,
        EmployeeRecord>> employees;

String fileType = options.getFileType();

if ("JSON".equalsIgnoreCase(fileType)) {

    LOGGER.info("Using JSON employee reader");

    employees =
            pipeline.apply(
                    "ReadEmployeeJson",
                    new ReadJsonEmployees(
                            options.getInputFile()
                    )
            );

} else if ("CSV".equalsIgnoreCase(fileType)) {

    LOGGER.info("Using CSV employee reader");

    employees =
            pipeline.apply(
                    "ReadEmployeeCsv",
                    new ReadCsvEmployees(
                            options.getInputFile()
                    )
            );

} else {

    throw new IllegalArgumentException(
            "Unsupported file type for employee ingestion: "
                    + fileType
    );
}



        /*
         * ------------------------------------------------------------
         * 2. VALIDATE
         * ------------------------------------------------------------
         */
        PCollectionTuple validationResults =
                employees.apply(
                        "ValidateEmployeeRecords",
                        ParDo.of(
                                new EmployeeValidationFn(
                                        options.getInputFile(),
                                        options.getExecutionId()
                                )
                        ).withOutputTags(
                                EmployeeValidationFn.VALID_RECORDS,
                                TupleTagList.of(
                                        EmployeeValidationFn.INVALID_RECORDS
                                )
                        )
                );

        PCollection<
                org.apache.beam.sdk.values.KV<
                        Long,
                        EmployeeRecord>> validEmployees =
                validationResults.get(
                        EmployeeValidationFn.VALID_RECORDS
                );

        PCollection<ValidationFailure> invalidEmployees =
                validationResults.get(
                        EmployeeValidationFn.INVALID_RECORDS
                );

        /*
         * ------------------------------------------------------------
         * 3. WRITE VALIDATION ERRORS TO TEXT FILE
         * ------------------------------------------------------------
         */
     invalidEmployees
        .apply(
                "FormatValidationErrors",
                ParDo.of(
                        new ErrorRecordFormatter(
                                options.getInputFile()
                        )
                )
        )
        .apply(
                "WriteValidationErrorsToText",
                org.apache.beam.sdk.io.TextIO
                        .write()
                        .to(options.getErrorOutput())
                        .withSuffix(".txt")
                        .withoutSharding()
        );



        /*
         * ------------------------------------------------------------
         * 4. CONVERT VALIDATION FAILURES TO LOAD FAILURE MODEL
         * -------------------------------------------------------
         * 
         * -----
         */
        PCollection<LoadFailure>
                validationLoadFailures =
                invalidEmployees.apply(
                        "ConvertValidationFailuresToLoadFailures",
                        ParDo.of(
                                new ValidationFailureToLoadFailureFn()
                        )
                );

        /*
         * ------------------------------------------------------------
         * 5. WRITE VALIDATION ERRORS TO POSTGRES
         * ------------------------------------------------------------
         */
        validationLoadFailures.apply(
                "WriteValidationErrorsToPostgres",
                ParDo.of(
                        new WriteIngestionErrorToPostgresFn(
                                options.getJdbcUrl(),
                                options.getJdbcUsername(),
                                options.getJdbcPassword()
                        )
                )
        );

        /*
         * ------------------------------------------------------------
         * 6. ADD METADATA
         * ------------------------------------------------------------
         */
        PCollection<
                com.amex.lumi.beam.model.EnrichedEmployeeRecord>
                enrichedEmployees =
                validEmployees.apply(
                        "AddIngestionMetadata",
                        ParDo.of(
                                new EnrichEmployeeMetadataFn(
                                        options.getExecutionId(),
                                        options.getIngestionTimestamp(),
                                        options.getSourceCreationTime(),
                                        options.getInputFile()
                                )
                        )
                );

        /*
         * ------------------------------------------------------------
         * 7. ENCRYPT SENSITIVE FIELDS
         * ------------------------------------------------------------
         *
         * Encrypt:
         *   phone_number
         *   salary
         *   emergency_contact.phone
         *
         * The emergency_contact object itself is NOT encrypted.
         */
        PCollection<EncryptedEmployeeRecord>
                encryptedEmployees =
                enrichedEmployees.apply(
                        "EncryptSensitiveFields",
                        ParDo.of(
                                new EncryptEmployeeFieldsFn(
                                        options.getEncryptionKey()
                                )
                        )
                );

        /*
         * ------------------------------------------------------------
         * 8. LOAD EMPLOYEE RECORDS
         * ------------------------------------------------------------
         */
        PCollectionTuple loadResults =
                encryptedEmployees.apply(
                        "WriteEmployeesToPostgres",
                        ParDo.of(
                                new WriteEmployeeToPostgresFn(
                                        options.getJdbcUrl(),
                                        options.getJdbcUsername(),
                                        options.getJdbcPassword()
                                )
                        ).withOutputTags(
                                WriteEmployeeToPostgresFn.LOADED_RECORDS,
                                TupleTagList.of(
                                        WriteEmployeeToPostgresFn.LOAD_FAILURES
                                )
                        )
                );

        PCollection<EncryptedEmployeeRecord>
                loadedEmployees =
                loadResults.get(
                        WriteEmployeeToPostgresFn.LOADED_RECORDS
                );

        PCollection<LoadFailure>
                loadFailures =
                loadResults.get(
                        WriteEmployeeToPostgresFn.LOAD_FAILURES
                );

        /*
         * ------------------------------------------------------------
         * 9. WRITE LOAD ERRORS TO TEXT
         * ------------------------------------------------------------
         */
        loadFailures
                .apply(
                        "FormatLoadErrors",
                        ParDo.of(
                                new LoadFailureFormatter()
                        )
                )
                .apply(
                        "WriteLoadErrorsToText",
                        org.apache.beam.sdk.io.TextIO
                                .write()
                                .to(
                                        options.getErrorOutput()
                                                + "-load"
                                )
                                .withSuffix(".txt")
                                .withoutSharding()
                );

        /*
         * ------------------------------------------------------------
         * 10. WRITE LOAD ERRORS TO POSTGRES
         * ------------------------------------------------------------
         */
        loadFailures.apply(
                "WriteLoadErrorsToPostgres",
                ParDo.of(
                        new WriteIngestionErrorToPostgresFn(
                                options.getJdbcUrl(),
                                options.getJdbcUsername(),
                                options.getJdbcPassword()
                        )
                )
        );

        /*
         * ------------------------------------------------------------
         * 11. COUNT SUCCESSFULLY LOADED RECORDS
         * ------------------------------------------------------------
         */
        PCollection<Long> loadedRecordIndicators =
                loadedEmployees.apply(
                        "CreateLoadedRecordIndicators",
                        ParDo.of(
                                new LoadedRecordCounter()
                        )
                );

        PCollection<Long> actualLoadedRecordCount =
                loadedRecordIndicators.apply(
                        "CountLoadedRecords",
                        Sum.longsGlobally()
                );

 /*
 * ------------------------------------------------------------
 * 12. CONTROL FILE VALIDATION
 * ------------------------------------------------------------
 *
 * Compare expected record count with actual loaded count.
 *
 * A mismatch is NOT thrown immediately because the FAILED
 * execution status must first be persisted.
 */
PCollectionTuple recordCountDecision =
        actualLoadedRecordCount.apply(
                "EvaluateRecordCount",
                ParDo.of(
                        new DoFn<Long, RecordCountCheckResult>() {

                            @ProcessElement
                            public void processElement(
                                    ProcessContext context) {

                                long actualCount =
                                        context.element();

                                RecordCountCheckResult result =
                                        RecordCountCheckResult.evaluate(
                                                expectedRecordCount,
                                                actualCount
                                        );

                                if (result.isMatched()) {

                                    context.output(
                                            MATCHED_COUNTS,
                                            result
                                    );

                                } else {

                                    context.output(
                                            MISMATCHED_COUNTS,
                                            result
                                    );
                                }
                            }
                        }
                ).withOutputTags(
                        MATCHED_COUNTS,
                        TupleTagList.of(
                                MISMATCHED_COUNTS
                        )
                )
        );

PCollection<RecordCountCheckResult> matchedCounts =
        recordCountDecision.get(
                MATCHED_COUNTS
        );

PCollection<RecordCountCheckResult> mismatchedCounts =
        recordCountDecision.get(
                MISMATCHED_COUNTS
        );



  /*
 * ------------------------------------------------------------
 * 13. EXECUTION SUCCESS STATUS
 * ------------------------------------------------------------
 *
 * SUCCESS is written ONLY when:
 *
 * expected count == actual loaded count
 */
String pipelineExecutionId =
        options.getExecutionId();

String pipelineInputFile =
        options.getInputFile();

String jdbcUrl =
        options.getJdbcUrl();

String jdbcUsername =
        options.getJdbcUsername();

String jdbcPassword =
        options.getJdbcPassword();

matchedCounts
        .apply(
                "CreateSuccessfulExecution",
                ParDo.of(
                        new DoFn<RecordCountCheckResult, IngestionExecution>() {

                            private final String executionId =
                                    pipelineExecutionId;

                            private final String sourceFile =
                                    pipelineInputFile;

                            private final long expectedCount =
                                    expectedRecordCount;

                            private final Instant pipelineStartedAt =
                                    startedAt;

                            @ProcessElement
                            public void processElement(
                                    ProcessContext context) {

                                RecordCountCheckResult result =
                                        context.element();

                                IngestionExecutionFactory factory =
                                        new IngestionExecutionFactory();

                                IngestionExecution success =
                                        factory.createSuccess(
                                                executionId,
                                                sourceFile,
                                                expectedCount,
                                                result.getActualLoadedRecordCount(),
                                                pipelineStartedAt,
                                                Instant.now()
                                        );

                                context.output(success);
                            }
                        }
                )
        )
        .apply(
                "WriteSuccessfulExecution",
                ParDo.of(
                        new WriteIngestionExecutionToPostgresFn(
                                jdbcUrl,
                                jdbcUsername,
                                jdbcPassword
                        )
                )
        );


   /*
 * ------------------------------------------------------------
 * 14. EXECUTION FAILED STATUS
 * ------------------------------------------------------------
 *
 * This branch executes when:
 *
 * expected count != actual loaded count
 *
 * FAILED is persisted before the pipeline is deliberately
 * failed.
 */
PCollection<Void> failedExecutionStatus =
        mismatchedCounts
                .apply(
                        "CreateFailedExecution",
                        ParDo.of(
                                new DoFn<RecordCountCheckResult, IngestionExecution>() {

                                    private final String executionId =
                                            pipelineExecutionId;

                                    private final String sourceFile =
                                            pipelineInputFile;

                                    private final long expectedCount =
                                            expectedRecordCount;

                                    private final Instant pipelineStartedAt =
                                            startedAt;

                                    @ProcessElement
                                    public void processElement(
                                            ProcessContext context) {

                                        RecordCountCheckResult result =
                                                context.element();

                                        String failureReason =
                                                result.getFailureMessage();

                                        IngestionExecutionFactory factory =
                                                new IngestionExecutionFactory();

                                        IngestionExecution failed =
                                                factory.createFailed(
                                                        executionId,
                                                        sourceFile,
                                                        expectedCount,
                                                        result.getActualLoadedRecordCount(),
                                                        pipelineStartedAt,
                                                        Instant.now(),
                                                        failureReason
                                                );

                                        context.output(failed);
                                    }
                                }
                        )
                )
                .apply(
                        "WriteFailedExecution",
                        ParDo.of(
                                new WriteIngestionExecutionToPostgresFn(
                                        jdbcUrl,
                                        jdbcUsername,
                                        jdbcPassword
                                )
                        )
                );


                mismatchedCounts
        .apply(
                "WaitForFailedStatusPersistence",
                Wait.on(failedExecutionStatus)
        )
        .apply(
                "FailPipelineAfterFailureStatusPersisted",
                ParDo.of(
                        new DoFn<RecordCountCheckResult, Void>() {

                            @ProcessElement
                            public void processElement(
                                    ProcessContext context) {

                                RecordCountCheckResult result =
                                        context.element();

                                throw new IllegalStateException(
                                        result.getFailureMessage()
                                );
                            }
                        }
                )
        );


       /*
 * ------------------------------------------------------------
 * 15. LOG PIPELINE SUBMISSION
 * ------------------------------------------------------------
 */
LOGGER.info(
        "Submitting employee ingestion pipeline"
);

/*
 * Start the Beam pipeline.
 */
PipelineResult result =
        pipeline.run();

/*
 * ------------------------------------------------------------
 * 16. WAIT FOR COMPLETION
 * ------------------------------------------------------------
 */
try {

    PipelineResult.State finalState =
            result.waitUntilFinish();

    if (finalState == PipelineResult.State.DONE) {

        LOGGER.info(
                "Employee ingestion pipeline completed successfully: executionId={}, state={}",
                options.getExecutionId(),
                finalState
        );

        return;
    }

    LOGGER.error(
            "Employee ingestion pipeline did not complete successfully: executionId={}, state={}",
            options.getExecutionId(),
            finalState
    );

    throw new IllegalStateException(
            "Employee ingestion pipeline failed with final state: "
                    + finalState
    );

} catch (Exception exception) {

    LOGGER.error(
            "Employee ingestion pipeline failed: executionId={}",
            options.getExecutionId(),
            exception
    );

    throw exception;
}


    }

    private static void writeExecutionStatus(
            IngestionExecution execution,
            IngestionPipelineOptions options) {

        IngestionExecutionRepository repository =
                new IngestionExecutionRepository(
                        options.getJdbcUrl(),
                        options.getJdbcUsername(),
                        options.getJdbcPassword()
                );

        try {

            repository.save(execution);

            LOGGER.info(
                    "Execution status persisted: executionId={}, status={}",
                    execution.getExecutionId(),
                    execution.getStatus()
            );

        } catch (Exception exception) {

            throw new IllegalStateException(
                    "Unable to persist ingestion execution status: "
                            + execution.getExecutionId(),
                    exception
            );
        }
    }

    private static void validateOptions(
            IngestionPipelineOptions options) {

        if (options.getInputFile() == null
                || options.getInputFile().isBlank()) {

            throw new IllegalArgumentException(
                    "inputFile must not be blank"
            );
        }

        if (options.getExecutionId() == null
                || options.getExecutionId().isBlank()) {

            throw new IllegalArgumentException(
                    "executionId must not be blank"
            );
        }

        if (options.getIngestionTimestamp() == null
                || options.getIngestionTimestamp().isBlank()) {

            throw new IllegalArgumentException(
                    "ingestionTimestamp must not be blank"
            );
        }

        if (options.getSourceCreationTime() == null
                || options.getSourceCreationTime().isBlank()) {

            throw new IllegalArgumentException(
                    "sourceCreationTime must not be blank"
            );
        }

        if (options.getErrorOutput() == null
                || options.getErrorOutput().isBlank()) {

            throw new IllegalArgumentException(
                    "errorOutput must not be blank"
            );
        }

        if (options.getEncryptionKey() == null
                || options.getEncryptionKey().isBlank()) {

            throw new IllegalArgumentException(
                    "encryptionKey must not be blank"
            );
        }

        if (options.getJdbcUrl() == null
                || options.getJdbcUrl().isBlank()) {

            throw new IllegalArgumentException(
                    "jdbcUrl must not be blank"
            );
        }

        if (options.getJdbcUsername() == null
                || options.getJdbcUsername().isBlank()) {

            throw new IllegalArgumentException(
                    "jdbcUsername must not be blank"
            );
        }

        if (options.getJdbcPassword() == null
                || options.getJdbcPassword().isBlank()) {

            throw new IllegalArgumentException(
                    "jdbcPassword must not be blank"
            );
        }

        if (options.getControlFile() == null
                || options.getControlFile().isBlank()) {

            throw new IllegalArgumentException(
                    "controlFile must not be blank"
            );
        }
    }
}
