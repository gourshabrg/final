package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.validation.ValidationFailure;
import org.apache.beam.sdk.transforms.DoFn;

public class ErrorRecordFormatter
        extends DoFn<ValidationFailure, String> {

    private final String sourceFile;

    public ErrorRecordFormatter(String sourceFile) {
        this.sourceFile = sourceFile;
    }

    @ProcessElement
    public void processElement(
            ProcessContext context) {

        ValidationFailure failure =
                context.element();

        String employeeId =
                failure.getEmployee()
                        .getEmployeeId();

        String errorMessage =
                String.join(
                        "; ",
                        failure.getErrors()
                );

        String errorRecord =
                "record_number="
                        + failure.getRecordNumber()
                        + "|error_type=VALIDATION_ERROR"
                        + "|error_message="
                        + sanitize(errorMessage)
                        + "|employee_id="
                        + sanitize(employeeId)
                        + "|source_file="
                        + sanitize(sourceFile);

        context.output(
                errorRecord
        );
    }

    private String sanitize(String value) {

        if (value == null) {
            return "";
        }

        return value
                .replace("|", " ")
                .replace("\r", " ")
                .replace("\n", " ");
    }
}
