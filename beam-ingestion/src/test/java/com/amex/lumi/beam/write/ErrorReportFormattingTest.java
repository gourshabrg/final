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
                        + "|employee_id=EMP0001|source_file=in.csv|execution_id=exec-1",
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
}
