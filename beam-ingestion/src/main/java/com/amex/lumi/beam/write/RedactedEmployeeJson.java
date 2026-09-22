package com.amex.lumi.beam.write;

import com.amex.lumi.beam.common.PipelineConstants;
import com.amex.lumi.beam.model.EmployeeRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Turns a rejected employee into JSON for the error table, with phone and salary hidden.
 */
final class RedactedEmployeeJson {

    private RedactedEmployeeJson() {
    }

    static ObjectNode from(EmployeeRecord employee, ObjectMapper mapper) {
        if (employee == null) {
            return null;
        }
        ObjectNode json = mapper.valueToTree(employee);
        redact(json, "phone_number");
        redact(json, "salary");
        JsonNode contact = json.get("emergency_contact");
        if (contact instanceof ObjectNode contactJson) {
            redact(contactJson, "phone");
        }
        return json;
    }

    private static void redact(ObjectNode json, String field) {
        if (json.hasNonNull(field)) {
            json.put(field, PipelineConstants.REDACTED);
        }
    }
}
