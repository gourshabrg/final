package com.amex.lumi.beam.pipeline;

import com.amex.lumi.beam.execution.RecordCountCheck;
import com.amex.lumi.beam.options.IngestionPipelineOptions;
import com.amex.lumi.beam.write.DatabaseConfig;
import org.apache.beam.sdk.Pipeline;

/** Test helper: builds and runs the real graph (IngestionGraph is package-private). */
public final class IngestionGraphRunner {

    private IngestionGraphRunner() {
    }

    public static void run(IngestionPipelineOptions options, RecordCountCheck.RunContext run) {
        DatabaseConfig database = new DatabaseConfig(
                options.getJdbcUrl(), options.getJdbcUsername(), options.getJdbcPassword());
        Pipeline pipeline = Pipeline.create(options);
        IngestionGraph.build(pipeline, options, database, run);
        pipeline.run().waitUntilFinish();
    }
}
