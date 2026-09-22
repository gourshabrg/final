package com.amex.lumi.beam.write;

import com.amex.lumi.beam.model.RecordFailure;

/**
 * One error line: record_number=4|error_type=...|error_message=...|employee_id=...
 */
public final class ErrorLineFormatter {

    private ErrorLineFormatter() {
    }

    public static String format(RecordFailure failure) {
        return "record_number=" + failure.recordNumber()
                + "|error_type=" + failure.type()
                + "|error_message=" + clean(failure.message())
                + "|employee_id=" + clean(failure.employeeId())
                + "|source_file=" + clean(failure.sourceFile())
                + "|execution_id=" + clean(failure.executionId());
    }

    // Keeps each failure on one line.
    static String clean(String value) {
        if (value == null) {
            return "";
        }
        return value.replace('|', ' ').replace('\r', ' ').replace('\n', ' ');
    }
}
