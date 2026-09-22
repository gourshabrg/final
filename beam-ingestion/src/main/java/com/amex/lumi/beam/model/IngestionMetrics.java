
package com.amex.lumi.beam.model;

import java.io.Serializable;

/**
 * Serializable aggregate counts describing one ingestion run.
 */
public class IngestionMetrics
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long parsedRecordCount;
    private final long validRecordCount;
    private final long validationFailureCount;
    private final long loadedRecordCount;
    private final long loadFailureCount;

    public IngestionMetrics(
            long parsedRecordCount,
            long validRecordCount,
            long validationFailureCount,
            long loadedRecordCount,
            long loadFailureCount) {

        this.parsedRecordCount = parsedRecordCount;
        this.validRecordCount = validRecordCount;
        this.validationFailureCount = validationFailureCount;
        this.loadedRecordCount = loadedRecordCount;
        this.loadFailureCount = loadFailureCount;
    }

    public long getParsedRecordCount() {
        return parsedRecordCount;
    }

    public long getValidRecordCount() {
        return validRecordCount;
    }

    public long getValidationFailureCount() {
        return validationFailureCount;
    }

    public long getLoadedRecordCount() {
        return loadedRecordCount;
    }

    public long getLoadFailureCount() {
        return loadFailureCount;
    }

    @Override
    public String toString() {

        return "IngestionMetrics{"
                + "parsedRecordCount="
                + parsedRecordCount
                + ", validRecordCount="
                + validRecordCount
                + ", validationFailureCount="
                + validationFailureCount
                + ", loadedRecordCount="
                + loadedRecordCount
                + ", loadFailureCount="
                + loadFailureCount
                + '}';
    }
}
