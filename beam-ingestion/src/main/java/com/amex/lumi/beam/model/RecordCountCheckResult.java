package com.amex.lumi.beam.model;

import java.io.Serializable;
import java.util.Objects;


/**
 * Comparison result for expected and loaded ingestion record counts.
 */
public class RecordCountCheckResult
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean matched;
    private final long expectedRecordCount;
    private final long actualLoadedRecordCount;

    private RecordCountCheckResult(
            boolean matched,
            long expectedRecordCount,
            long actualLoadedRecordCount) {

        this.matched = matched;
        this.expectedRecordCount =
                expectedRecordCount;
        this.actualLoadedRecordCount =
                actualLoadedRecordCount;
    }

    /**
     * Evaluates whether the expected and actual counts match.
     *
     * @param expectedRecordCount count declared by the control file
     * @param actualLoadedRecordCount count loaded by the pipeline
     * @return comparison result containing both counts
     */
    public static RecordCountCheckResult evaluate(
            long expectedRecordCount,
            long actualLoadedRecordCount) {

        return new RecordCountCheckResult(
                expectedRecordCount
                        == actualLoadedRecordCount,
                expectedRecordCount,
                actualLoadedRecordCount
        );
    }

    public boolean isMatched() {
        return matched;
    }

    public long getExpectedRecordCount() {
        return expectedRecordCount;
    }

    public long getActualLoadedRecordCount() {
        return actualLoadedRecordCount;
    }

    public String getFailureMessage() {

        return "Record count mismatch: expected="
                + expectedRecordCount
                + ", actual="
                + actualLoadedRecordCount;
    }

    @Override
    public String toString() {

        return "RecordCountCheckResult{"
                + "matched="
                + matched
                + ", expectedRecordCount="
                + expectedRecordCount
                + ", actualLoadedRecordCount="
                + actualLoadedRecordCount
                + '}';
    }
    @Override
public boolean equals(Object o) {

    if (this == o) {
        return true;
    }

    if (!(o instanceof RecordCountCheckResult that)) {
        return false;
    }

    return matched == that.matched
            && expectedRecordCount == that.expectedRecordCount
            && actualLoadedRecordCount == that.actualLoadedRecordCount;
}

@Override
public int hashCode() {

    return Objects.hash(
            matched,
            expectedRecordCount,
            actualLoadedRecordCount
    );
}

}
