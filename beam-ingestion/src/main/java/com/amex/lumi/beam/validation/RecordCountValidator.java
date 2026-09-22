

package com.amex.lumi.beam.validation;

public class RecordCountValidator {

    public void validate(
            long expectedRecordCount,
            long actualLoadedRecordCount) {

        if (expectedRecordCount != actualLoadedRecordCount) {

            throw new IllegalStateException(
                    "Record count mismatch: expected="
                            + expectedRecordCount
                            + ", actual="
                            + actualLoadedRecordCount
            );
        }
    }
}
