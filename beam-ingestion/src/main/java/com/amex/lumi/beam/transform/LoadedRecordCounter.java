package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.model.EncryptedEmployeeRecord;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Emits one count marker for each successfully loaded employee record.
 */
public class LoadedRecordCounter
        extends DoFn<EncryptedEmployeeRecord, Long> {

    @ProcessElement
    public void processElement(
            ProcessContext context) {

        context.output(1L);
    }
}
