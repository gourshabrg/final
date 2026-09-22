package com.amex.lumi.beam.validate;

import com.amex.lumi.beam.common.IngestionMetrics;
import com.amex.lumi.beam.model.ParsedEmployee;
import com.amex.lumi.beam.model.RecordFailure;
import com.amex.lumi.beam.model.RecordFailure.FailureType;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Splits records into VALID and INVALID. Invalid ones go to the error report, not dropped.
 */
public class ValidateEmployees extends PTransform<PCollection<ParsedEmployee>, PCollectionTuple> {

    public static final TupleTag<ParsedEmployee> VALID = new TupleTag<>() {
    };
    public static final TupleTag<RecordFailure> INVALID = new TupleTag<>() {
    };

    private final String executionId;

    public ValidateEmployees(String executionId) {
        this.executionId = executionId;
    }

    @Override
    public PCollectionTuple expand(PCollection<ParsedEmployee> input) {
        return input.apply("ApplyValidationRules",
                ParDo.of(new ValidateFn(executionId)).withOutputTags(VALID, TupleTagList.of(INVALID)));
    }

    static class ValidateFn extends DoFn<ParsedEmployee, ParsedEmployee> {

        private static final Logger LOGGER = LoggerFactory.getLogger(ValidateFn.class);

        private final Counter validationErrors = IngestionMetrics.counter(IngestionMetrics.VALIDATION_ERRORS);
        private final String executionId;
        private transient EmployeeValidator validator;

        ValidateFn(String executionId) {
            this.executionId = executionId;
        }

        @Setup
        public void setup() {
            validator = new EmployeeValidator();
        }

        @ProcessElement
        public void processElement(@Element ParsedEmployee record, MultiOutputReceiver out) {
            List<String> errors = validator.validate(record.employee());
            if (errors.isEmpty()) {
                out.get(VALID).output(record);
                return;
            }
            validationErrors.inc();
            String message = String.join("; ", errors);
            LOGGER.warn("Record {} (employee_id={}) failed validation: {}",
                    record.recordNumber(), record.employee().getEmployeeId(), message);
            out.get(INVALID).output(new RecordFailure(record.recordNumber(), record.sourceFile(),
                    executionId, FailureType.VALIDATION_ERROR, message, record.employee()));
        }
    }
}
