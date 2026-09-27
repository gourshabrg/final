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

class RejectDuplicateIdsTest {

    @Test
    void keepsTheFirstRecordOfEachIdAndRejectsTheRest() {
        EmployeeRecord other = TestEmployees.valid();
        other = other.toBuilder().employeeId("EMP0002").build();
        Instant now = Instant.now();

        Pipeline pipeline = Pipeline.create();
        PCollectionTuple result = pipeline
                .apply(Create.of(
                        new ParsedEmployee(7, "in.csv", now, TestEmployees.valid()),
                        new ParsedEmployee(2, "in.csv", now, TestEmployees.valid()),
                        new ParsedEmployee(5, "in.csv", now, other)))
                .apply(new RejectDuplicateIds("exec-1"));

        PAssert.that(result.get(RejectDuplicateIds.UNIQUE)
                        .apply("UniqueNumbers", MapElements.into(TypeDescriptors.longs())
                                .via(ParsedEmployee::recordNumber)))
                .containsInAnyOrder(2L, 5L);
        PAssert.that(result.get(RejectDuplicateIds.DUPLICATES)
                        .apply("DuplicateSummary", MapElements.into(TypeDescriptors.strings())
                                .via(failure -> failure.recordNumber() + ":" + failure.type() + ":"
                                        + failure.message())))
                .containsInAnyOrder("7:" + FailureType.DUPLICATE_ERROR
                        + ":employee_id EMP0001 already appears in record 2; only the first record is loaded");

        pipeline.run().waitUntilFinish();
    }
}
