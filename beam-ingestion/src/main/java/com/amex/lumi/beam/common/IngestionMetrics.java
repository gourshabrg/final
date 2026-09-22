package com.amex.lumi.beam.common;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.slf4j.Logger;

/**
 * Beam counters per stage, printed as one summary line at the end (instead of one log line per record).
 */
public final class IngestionMetrics {

    private static final String NAMESPACE = "lumi-ingestion";

    public static final String RECORDS_PARSED = "records_parsed";
    public static final String PARSE_ERRORS = "parse_errors";
    public static final String VALIDATION_ERRORS = "validation_errors";
    public static final String RECORDS_LOADED = "records_loaded";
    public static final String LOAD_ERRORS = "load_errors";

    private IngestionMetrics() {
    }

    public static Counter counter(String name) {
        return Metrics.counter(NAMESPACE, name);
    }

    public static void logSummary(PipelineResult result, String executionId, Logger logger) {
        logger.info("Ingestion summary executionId={} parsed={} parse_errors={} validation_errors={} loaded={} load_errors={}",
                executionId,
                read(result, RECORDS_PARSED),
                read(result, PARSE_ERRORS),
                read(result, VALIDATION_ERRORS),
                read(result, RECORDS_LOADED),
                read(result, LOAD_ERRORS));
    }

    private static long read(PipelineResult result, String name) {
        MetricQueryResults query = result.metrics().queryMetrics(
                MetricsFilter.builder().addNameFilter(MetricNameFilter.named(NAMESPACE, name)).build());
        long total = 0;
        for (MetricResult<Long> counter : query.getCounters()) {
            total += counter.getAttempted();
        }
        return total;
    }
}
