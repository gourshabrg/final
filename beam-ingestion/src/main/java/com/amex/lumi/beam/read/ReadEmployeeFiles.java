package com.amex.lumi.beam.read;

import com.amex.lumi.beam.common.IngestionMetrics;
import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.model.FileType;
import com.amex.lumi.beam.model.ParsedEmployee;
import com.amex.lumi.beam.model.RecordFailure;
import com.amex.lumi.beam.model.RecordFailure.FailureType;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Reads one file, or all split files matching a pattern like *.csv.
 * Good records go to PARSED, records that cannot be read go to PARSE_FAILURES.
 */
public class ReadEmployeeFiles extends PTransform<PBegin, PCollectionTuple> {

    public static final TupleTag<ParsedEmployee> PARSED = new TupleTag<>() {
    };
    public static final TupleTag<RecordFailure> PARSE_FAILURES = new TupleTag<>() {
    };

    private final String filePattern;
    private final FileType fileType;
    private final String executionId;

    public ReadEmployeeFiles(String filePattern, FileType fileType, String executionId) {
        this.filePattern = filePattern;
        this.fileType = fileType;
        this.executionId = executionId;
    }

    @Override
    public PCollectionTuple expand(PBegin input) {
        return input
                .apply("MatchInputFiles", FileIO.match().filepattern(filePattern))
                .apply("OpenInputFiles", FileIO.readMatches())
                .apply("Parse" + fileType.name() + "Records",
                        ParDo.of(new ParseFileFn(EmployeeFileParsers.forType(fileType), fileType, executionId))
                                .withOutputTags(PARSED, TupleTagList.of(PARSE_FAILURES)));
    }

    /** Parses one file and numbers its records 1, 2, 3... */
    static class ParseFileFn extends DoFn<FileIO.ReadableFile, ParsedEmployee> {

        private static final Logger LOGGER = LoggerFactory.getLogger(ParseFileFn.class);
        private static final char BYTE_ORDER_MARK = '\uFEFF';

        private final Counter parsed = IngestionMetrics.counter(IngestionMetrics.RECORDS_PARSED);
        private final Counter parseErrors = IngestionMetrics.counter(IngestionMetrics.PARSE_ERRORS);

        private final EmployeeFileParser parser;
        private final FileType fileType;
        private final String executionId;

        ParseFileFn(EmployeeFileParser parser, FileType fileType, String executionId) {
            this.parser = parser;
            this.fileType = fileType;
            this.executionId = executionId;
        }

        @ProcessElement
        public void processElement(@Element FileIO.ReadableFile file, MultiOutputReceiver out) throws IOException {
            String sourceFile = file.getMetadata().resourceId().toString();
            // source_creation_time = time the file was parsed.
            Instant parsedAt = Instant.now();
            LOGGER.info("Parsing {} file: {}", fileType, sourceFile);

            FileEmitter emitter = new FileEmitter(sourceFile, parsedAt, out);
            try (Reader reader = skipByteOrderMark(
                    Channels.newReader(file.open(), StandardCharsets.UTF_8))) {
                parser.parse(reader, emitter);
            }

            LOGGER.info("Finished parsing {}: {} record(s) read, {} could not be parsed",
                    sourceFile, emitter.goodRecords, emitter.badRecords);
        }

        // Files saved on Windows may start with an invisible character (BOM) that would break the first header name.
        private static Reader skipByteOrderMark(Reader reader) throws IOException {
            PushbackReader pushback = new PushbackReader(reader, 1);
            int first = pushback.read();
            if (first != -1 && first != BYTE_ORDER_MARK) {
                pushback.unread(first);
            }
            return pushback;
        }

        /** Sends parser results to the Beam outputs. */
        private final class FileEmitter implements RecordHandler {

            private final String sourceFile;
            private final Instant parsedAt;
            private final MultiOutputReceiver out;
            private long recordNumber;
            private long goodRecords;
            private long badRecords;

            FileEmitter(String sourceFile, Instant parsedAt, MultiOutputReceiver out) {
                this.sourceFile = sourceFile;
                this.parsedAt = parsedAt;
                this.out = out;
            }

            @Override
            public void onRecord(EmployeeRecord employee) {
                recordNumber++;
                goodRecords++;
                parsed.inc();
                out.get(PARSED).output(new ParsedEmployee(recordNumber, sourceFile, parsedAt, employee));
            }

            @Override
            public void onError(String reason) {
                recordNumber++;
                badRecords++;
                parseErrors.inc();
                LOGGER.warn("Record {} in {} could not be parsed: {}", recordNumber, sourceFile, reason);
                out.get(PARSE_FAILURES).output(new RecordFailure(
                        recordNumber, sourceFile, executionId, FailureType.PARSE_ERROR, reason, null));
            }
        }
    }
}
