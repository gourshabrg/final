package com.amex.lumi.beam.model;

import java.io.Serializable;

/**
 * Validated metadata read from an ingestion control file.
 */
public class IngestionControl
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long expectedRecordCount;

    public IngestionControl(
            long expectedRecordCount) {

        this.expectedRecordCount =
                expectedRecordCount;
    }

    public long getExpectedRecordCount() {
        return expectedRecordCount;
    }

    @Override
    public String toString() {

        return "IngestionControl{"
                + "expectedRecordCount="
                + expectedRecordCount
                + '}';
    }
}
