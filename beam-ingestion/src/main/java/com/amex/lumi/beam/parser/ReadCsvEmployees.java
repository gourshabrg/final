package com.amex.lumi.beam.parser;

import com.amex.lumi.beam.model.EmployeeRecord;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;

/**
 * Reads header-based CSV employee files and emits numbered employee records.
 */
public class ReadCsvEmployees
        extends PTransform<PBegin, PCollection<KV<Long, EmployeeRecord>>> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ReadCsvEmployees.class);

    private final String inputFile;

    public ReadCsvEmployees(String inputFile) {
        this.inputFile = inputFile;
    }

        /**
         * Builds the Beam file-match, read, and CSV parse stages.
         *
         * @param input pipeline input boundary
         * @return employee records keyed by source record number
         */
        @Override
    public PCollection<KV<Long, EmployeeRecord>> expand(PBegin input) {

        return input
        .apply(
                "MatchCsvFiles",
                FileIO.match()
                        .filepattern(inputFile)
        )
        .apply(
                "ReadCsvFiles",
                FileIO.readMatches()
        )
        .apply(
                "ParseCsvEmployees",
                ParDo.of(new ParseCsvFileFn())
        )
        .setTypeDescriptor(
                new TypeDescriptor<KV<Long, EmployeeRecord>>() {
                }
        );

    }

    private static class ParseCsvFileFn
            extends DoFn<FileIO.ReadableFile, KV<Long, EmployeeRecord>> {

        private transient CsvEmployeeParser parser;

        @Setup
        public void setup() {
            parser = new CsvEmployeeParser();
        }

        @ProcessElement
        public void processElement(ProcessContext context)
                throws IOException {

            FileIO.ReadableFile file = context.element();

            LOGGER.info(
                    "Reading CSV file: {}",
                    file.getMetadata().resourceId()
            );

            CSVFormat format = CSVFormat.DEFAULT
                    .builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .build();

            try (
                    ReadableByteChannel channel = file.open();
                    Reader reader = Channels.newReader(
                            channel,
                            StandardCharsets.UTF_8
                    );
                    CSVParser csvParser = format.parse(reader)
            ) {

                for (CSVRecord csvRecord : csvParser) {

                    long recordNumber = csvRecord.getRecordNumber();

                    EmployeeRecord employee =
                            parser.parse(csvRecord);

                    context.output(
                            KV.of(recordNumber, employee)
                    );
                }
            }

            LOGGER.info(
                    "Completed reading CSV file: {}",
                    file.getMetadata().resourceId()
            );
        }
    }
}
