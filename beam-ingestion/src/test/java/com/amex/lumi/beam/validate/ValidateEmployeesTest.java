package com.amex.lumi.beam.validate;

import com.amex.lumi.beam.TestEmployees;
import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.model.ParsedEmployee;
import com.amex.lumi.beam.model.RecordFailure.FailureType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.jupiter.api.Test;

import java.time.Instant;

class ValidateEmployeesTest {

    @Test
    void splitsValidAndInvalidRecords() {
        EmployeeRecord bad = TestEmployees.valid();
        bad.setEmployeeId("E1");
        Instant now = Instant.now();

        Pipeline pipeline = Pipeline.create();
        PCollectionTuple result = pipeline
                .apply(Create.of(
                        new ParsedEmployee(1, "in.csv", now, TestEmployees.valid()),
                        new ParsedEmployee(2, "in.csv", now, bad)))
                .apply(new ValidateEmployees("exec-1"));

        PAssert.that(result.get(ValidateEmployees.VALID)
                        .apply("ValidNumbers", MapElements.into(TypeDescriptors.longs())
                                .via(ParsedEmployee::recordNumber)))
                .containsInAnyOrder(1L);
        PAssert.that(result.get(ValidateEmployees.INVALID)
                        .apply("InvalidSummary", MapElements.into(TypeDescriptors.strings())
                                .via(failure -> failure.recordNumber() + ":" + failure.type() + ":" + failure.message())))
                .containsInAnyOrder("2:" + FailureType.VALIDATION_ERROR
                        + ":employee_id must be exactly 7 characters (found 2)");

        pipeline.run().waitUntilFinish();
    }
}
