package com.amex.lumi.beam.validation;

import com.amex.lumi.beam.model.RecordCountCheckResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates the actual loaded record count against the expected
 * record count from the control file.
 *
 * This DoFn does not throw for a mismatch because the pipeline
 * must first persist the FAILED execution status.
 */
public class RecordCountDecisionFn
        extends DoFn<Long, RecordCountCheckResult> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(RecordCountDecisionFn.class);

    private final long expectedRecordCount;

    public RecordCountDecisionFn(long expectedRecordCount) {
        this.expectedRecordCount = expectedRecordCount;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {

        long actualRecordCount = context.element();

        LOGGER.info(
                "Evaluating ingestion record count: expected={}, actual={}",
                expectedRecordCount,
                actualRecordCount
        );

        RecordCountCheckResult result =
                RecordCountCheckResult.evaluate(
                        expectedRecordCount,
                        actualRecordCount
                );

        if (result.isMatched()) {
            LOGGER.info(
                    "Record count validation succeeded: expected={}, actual={}",
                    expectedRecordCount,
                    actualRecordCount
            );
        } else {
            LOGGER.error(
                    "Record count validation failed: expected={}, actual={}",
                    expectedRecordCount,
                    actualRecordCount
            );
        }

        context.output(result);
    }
}
