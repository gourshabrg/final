package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.sink.LoadFailure;
import com.amex.lumi.beam.validation.ValidationFailure;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Converts validation failures into the common persistence-failure model.
 */
public class ValidationFailureToLoadFailureFn
        extends DoFn<ValidationFailure, LoadFailure> {

        /**
         * Maps one validation failure to a load-failure output element.
         *
         * @param context Beam processing context
         */
    @ProcessElement
    public void processElement(
            ProcessContext context) {

        ValidationFailure failure =
                context.element();

        LoadFailure loadFailure =
                new LoadFailure(
                        failure.getEmployee(),
                        failure.getRecordNumber(),
                        failure.getSourceFile(),
                        "VALIDATION_ERROR",
                        String.join(
                                "; ",
                                failure.getErrors()
                        ),
                        failure.getExecutionId()
                );

        context.output(loadFailure);
    }
}
