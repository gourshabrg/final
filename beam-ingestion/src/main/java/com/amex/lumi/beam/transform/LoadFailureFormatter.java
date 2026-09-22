package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.sink.LoadFailure;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Formats load failures as delimiter-safe operational error records.
 */
public class LoadFailureFormatter
        extends DoFn<LoadFailure, String> {

        /**
         * Emits one sanitized text record for a load failure.
         *
         * @param context Beam processing context
         */
    @ProcessElement
    public void processElement(
            ProcessContext context) {

        LoadFailure failure =
                context.element();

        String employeeId =
                failure.getEmployee()
                        .getEmployeeId();

        String errorRecord =
                "record_number="
                        + failure.getRecordNumber()
                        + "|error_type="
                        + sanitize(
                                failure.getErrorType()
                        )
                        + "|error_message="
                        + sanitize(
                                failure.getErrorMessage()
                        )
                        + "|employee_id="
                        + sanitize(
                                employeeId
                        )
                        + "|source_file="
                        + sanitize(
                                failure.getSourceFile()
                        )
                        + "|execution_id="
                        + sanitize(
                                failure.getExecutionId()
                        );

        context.output(errorRecord);
    }

    private String sanitize(
            String value) {

        if (value == null) {
            return "";
        }

        return value
                .replace("|", " ")
                .replace("\r", " ")
                .replace("\n", " ");
    }
}
