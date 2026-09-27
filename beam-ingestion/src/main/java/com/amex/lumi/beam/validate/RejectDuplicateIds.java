package com.amex.lumi.beam.validate;

import com.amex.lumi.beam.common.IngestionMetrics;
import com.amex.lumi.beam.model.ParsedEmployee;
import com.amex.lumi.beam.model.RecordFailure;
import com.amex.lumi.beam.model.RecordFailure.FailureType;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Keeps the first record per employee_id; later duplicates go to the error report.
 */
public class RejectDuplicateIds extends PTransform<PCollection<ParsedEmployee>, PCollectionTuple> {

    public static final TupleTag<ParsedEmployee> UNIQUE = new TupleTag<>() {
    };
    public static final TupleTag<RecordFailure> DUPLICATES = new TupleTag<>() {
    };

    private final String executionId;

    public RejectDuplicateIds(String executionId) {
        this.executionId = executionId;
    }

    @Override
    public PCollectionTuple expand(PCollection<ParsedEmployee> valid) {
        return valid
                .apply("KeyByEmployeeId", WithKeys.of((ParsedEmployee record) -> record.employee().getEmployeeId())
                        .withKeyType(TypeDescriptors.strings()))
                .apply("GroupByEmployeeId", GroupByKey.create())
                .apply("KeepFirstRecord", ParDo.of(new KeepFirstFn(executionId))
                        .withOutputTags(UNIQUE, TupleTagList.of(DUPLICATES)));
    }

    static class KeepFirstFn extends DoFn<KV<String, Iterable<ParsedEmployee>>, ParsedEmployee> {

        private static final Logger LOGGER = LoggerFactory.getLogger(KeepFirstFn.class);
        // Record number first; the file names only break ties between split part files.
        private static final Comparator<ParsedEmployee> FILE_ORDER = Comparator
                .comparingLong(ParsedEmployee::recordNumber)
                .thenComparing(ParsedEmployee::sourceFile)
                .thenComparing(ParsedEmployee::splitFile, Comparator.nullsFirst(Comparator.naturalOrder()));

        private final Counter duplicates = IngestionMetrics.counter(IngestionMetrics.DUPLICATE_RECORDS);
        private final String executionId;

        KeepFirstFn(String executionId) {
            this.executionId = executionId;
        }

        @ProcessElement
        public void processElement(@Element KV<String, Iterable<ParsedEmployee>> group, MultiOutputReceiver out) {
            List<ParsedEmployee> records = new ArrayList<>();
            group.getValue().forEach(records::add);
            records.sort(FILE_ORDER);

            ParsedEmployee first = records.get(0);
            out.get(UNIQUE).output(first);
            for (ParsedEmployee duplicate : records.subList(1, records.size())) {
                duplicates.inc();
                String message = "employee_id " + group.getKey() + " already appears in record "
                        + first.recordNumber() + "; only the first record is loaded";
                LOGGER.warn("Record {}: {}", duplicate.recordNumber(), message);
                out.get(DUPLICATES).output(new RecordFailure(duplicate.recordNumber(), duplicate.sourceFile(),
                        duplicate.splitFile(), executionId, FailureType.DUPLICATE_ERROR, message,
                        duplicate.employee()));
            }
        }
    }
}
