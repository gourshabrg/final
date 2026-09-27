package com.amex.lumi.beam.write;

import com.amex.lumi.beam.TestEmployees;
import com.amex.lumi.beam.model.RecordFailure;
import com.amex.lumi.beam.model.RecordFailure.FailureType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ErrorReportFormattingTest {

    @Test
    void formatsOneLinePerFailure() {
        RecordFailure failure = new RecordFailure(4, "in.csv", "exec-1", FailureType.VALIDATION_ERROR,
                "email is required|bad\nline", TestEmployees.valid());

        assertEquals("record_number=4|error_type=VALIDATION_ERROR|error_message=email is required bad line"
                        + "|employee_id=EMP0001|source_file=in.csv|split_file=|execution_id=exec-1",
                ErrorLineFormatter.format(failure));
    }

    @Test
    void redactsSensitiveFields() {
        ObjectNode json = RedactedEmployeeJson.from(TestEmployees.valid(), new ObjectMapper());

        assertEquals("[REDACTED]", json.get("phone_number").asText());
        assertEquals("[REDACTED]", json.get("salary").asText());
        assertEquals("[REDACTED]", json.get("emergency_contact").get("phone").asText());
        assertEquals("EMP0001", json.get("employee_id").asText());
    }

    @Test
    void splitRunNamesOriginalFileAndPartFile() {
        RecordFailure failure = new RecordFailure(87, "employees_large.csv", "split/part-00001.csv", "exec-1",
                FailureType.PARSE_ERROR, "salary must be a whole number", null);

        assertEquals("record_number=87|error_type=PARSE_ERROR|error_message=salary must be a whole number"
                        + "|employee_id=|source_file=employees_large.csv|split_file=split/part-00001.csv"
                        + "|execution_id=exec-1",
                ErrorLineFormatter.format(failure));
    }
}
