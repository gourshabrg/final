package com.amex.lumi.beam.model;

import java.io.Serializable;

/**
 * Values from the control file.
 */
public record IngestionControl(long expectedRecordCount) implements Serializable {
}
