
package com.amex.lumi.beam.validation;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates the loaded record count against the control-file expectation.
 */
public class RecordCountValidationFn
        extends DoFn<Long, Long> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(RecordCountValidationFn.class);

    private final long expectedRecordCount;

    public RecordCountValidationFn(long expectedRecordCount) {
        this.expectedRecordCount = expectedRecordCount;
    }

        /**
         * Validates and forwards the aggregate loaded count.
         *
         * @param context Beam processing context
         */
        @ProcessElement
    public void processElement(ProcessContext context) {

        long actualLoadedRecordCount = context.element();

        LOGGER.info(
                "Validating ingestion record count: expected={}, actual={}",
                expectedRecordCount,
                actualLoadedRecordCount
        );

        RecordCountValidator validator =
                new RecordCountValidator();

        validator.validate(
                expectedRecordCount,
                actualLoadedRecordCount
        );

        LOGGER.info(
                "Record count validation passed: expected={}, actual={}",
                expectedRecordCount,
                actualLoadedRecordCount
        );

        context.output(actualLoadedRecordCount);
    }
}
