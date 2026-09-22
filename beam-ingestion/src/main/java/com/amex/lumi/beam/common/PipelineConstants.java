package com.amex.lumi.beam.common;

/**
 * Values used in more than one place in the Beam job.
 */
public final class PipelineConstants {

    /** All times are stored in UTC so every machine writes the same value. */
    public static final String TIME_ZONE = "UTC";

    /** Text put in place of a missing value (Phase 1 rule). */
    public static final String MISSING_VALUE = " ";

    /** Text put in place of phone or salary in the error table. */
    public static final String REDACTED = "[REDACTED]";

    /** Separator between skills in a CSV cell, e.g. "Java;SQL". */
    public static final String SKILL_SEPARATOR = ";";

    private PipelineConstants() {
    }
}
