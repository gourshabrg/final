package com.amex.lumi.beam.model;

import java.io.Serializable;

/**
 * Values from the control file. fileName and sha256 are optional (null when not given).
 */
public record IngestionControl(long expectedRecordCount, String fileName, String sha256) implements Serializable {

    public IngestionControl(long expectedRecordCount) {
        this(expectedRecordCount, null, null);
    }
}
