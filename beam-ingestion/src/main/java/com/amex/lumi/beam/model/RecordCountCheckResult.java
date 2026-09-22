package com.amex.lumi.beam.model;

import java.io.Serializable;

/**
 * Expected count (control file) vs loaded count.
 */
public record RecordCountCheckResult(long expectedRecordCount, long actualLoadedRecordCount)
        implements Serializable {

    public boolean matched() {
        return expectedRecordCount == actualLoadedRecordCount;
    }

    public String failureMessage() {
        return "Record count mismatch: control file expected " + expectedRecordCount
                + " record(s) but " + actualLoadedRecordCount + " record(s) were loaded";
    }
}
