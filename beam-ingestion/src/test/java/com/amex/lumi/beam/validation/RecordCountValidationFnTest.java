package com.amex.lumi.beam.validation;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.jupiter.api.Test;

class RecordCountValidationFnTest {

    @Rule
    public final transient TestPipeline pipeline =
            TestPipeline.create();

    @Test
    void shouldPassWhenExpectedAndActualCountsMatch() {

        var actualCount =
                pipeline
                        .apply(
                                Create.of(5L)
                        )
                        .apply(
                                "ValidateRecordCount",
                                ParDo.of(
                                        new RecordCountValidationFn(5)
                                )
                        );

        PAssert.that(actualCount)
                .containsInAnyOrder(5L);
    }
}
