package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.model.EnrichedEmployeeRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.time.Instant;

public class EnrichEmployeeMetadataFn
        extends DoFn<KV<Long, EmployeeRecord>, EnrichedEmployeeRecord> {

    private final String executionId;
    private final String ingestionTimestamp;
    private final String sourceCreationTime;
    private final String sourceFile;

    private transient Instant parsedIngestionTimestamp;
    private transient Instant parsedSourceCreationTime;

    public EnrichEmployeeMetadataFn(
            String executionId,
            String ingestionTimestamp,
            String sourceCreationTime,
            String sourceFile) {

        this.executionId = executionId;
        this.ingestionTimestamp = ingestionTimestamp;
        this.sourceCreationTime = sourceCreationTime;
        this.sourceFile = sourceFile;
    }

    @Setup
    public void setup() {

        parsedIngestionTimestamp =
                Instant.parse(ingestionTimestamp);

        parsedSourceCreationTime =
                Instant.parse(sourceCreationTime);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {

        KV<Long, EmployeeRecord> input = context.element();

        long recordNumber = input.getKey();
        EmployeeRecord employee = input.getValue();

        EnrichedEmployeeRecord enrichedEmployee =
                new EnrichedEmployeeRecord(
                        employee,
                        recordNumber,
                        sourceFile,
                        executionId,
                        parsedIngestionTimestamp,
                        parsedSourceCreationTime
                );

        context.output(enrichedEmployee);
    }
}
