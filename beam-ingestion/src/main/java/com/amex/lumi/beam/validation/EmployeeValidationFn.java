package com.amex.lumi.beam.validation;

import com.amex.lumi.beam.model.EmployeeRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Routes employee records to valid and invalid Beam outputs.
 */
public class EmployeeValidationFn
        extends DoFn<KV<Long, EmployeeRecord>, KV<Long, EmployeeRecord>> {

    public static final TupleTag<KV<Long, EmployeeRecord>> VALID_RECORDS =
            new TupleTag<>() {};

    public static final TupleTag<ValidationFailure> INVALID_RECORDS =
            new TupleTag<>() {};

    private final String sourceFile;
    private final String executionId;

    private transient EmployeeValidator validator;

    public EmployeeValidationFn(
            String sourceFile,
            String executionId) {

        this.sourceFile = sourceFile;
        this.executionId = executionId;
    }

    @Setup
    public void setup() {

        validator = new EmployeeValidator();
    }

        /**
         * Validates one record and preserves its source record number.
         *
         * @param context Beam processing context
         */
        @ProcessElement
    public void processElement(
            ProcessContext context) {

        KV<Long, EmployeeRecord> input =
                context.element();

        long recordNumber =
                input.getKey();

        EmployeeRecord employee =
                input.getValue();

        ValidationResult result =
                validator.validate(employee);

        if (result.isValid()) {

            context.output(
                    VALID_RECORDS,
                    KV.of(
                            recordNumber,
                            employee
                    )
            );

            return;
        }

        ValidationFailure failure =
                new ValidationFailure(
                        employee,
                        recordNumber,
                        result.getErrors(),
                        sourceFile,
                        executionId
                );

        context.output(
                INVALID_RECORDS,
                failure
        );
    }
}
