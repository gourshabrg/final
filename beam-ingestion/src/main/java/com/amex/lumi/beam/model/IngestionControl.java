package com.amex.lumi.beam.model;

import java.io.Serializable;

/**
 * Values from the control file (record_count).
 */
public record IngestionControl(long expectedRecordCount) implements Serializable {
}
