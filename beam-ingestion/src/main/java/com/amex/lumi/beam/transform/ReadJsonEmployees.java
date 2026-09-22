package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.parser.JsonEmployeeParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Reads employee JSON files and emits numbered employee records.
 *
 * <p>Both JSON arrays and newline-delimited JSON documents are supported.</p>
 */
public class ReadJsonEmployees
        extends PTransform<PBegin, PCollection<KV<Long, EmployeeRecord>>> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ReadJsonEmployees.class);

    private final String filePattern;

    public ReadJsonEmployees(String filePattern) {
        this.filePattern = filePattern;

        LOGGER.info(
                "Initialized JSON employee reader with file pattern: {}",
                filePattern
        );
    }

        /**
         * Builds the Beam file-match, read, and parse stages.
         *
         * @param input pipeline input boundary
         * @return employee records keyed by source record number
         */
        @Override
    public PCollection<KV<Long, EmployeeRecord>> expand(PBegin input) {

        LOGGER.info(
                "Building Beam transformation for JSON input pattern: {}",
                filePattern
        );

        PCollection<KV<Long, EmployeeRecord>> employees =
                input
                        .apply(
                                "MatchEmployeeJsonFiles",
                                FileIO.match().filepattern(filePattern)
                        )
                        .apply(
                                "ReadEmployeeJsonFiles",
                                FileIO.readMatches()
                        )
                        .apply(
                                "ParseEmployeeJson",
                                ParDo.of(new ParseEmployeeFileFn())
                        );

        employees.setCoder(
                KvCoder.of(
                        VarLongCoder.of(),
                        SerializableCoder.of(EmployeeRecord.class)
                )
        );

        return employees;
    }

    private static class ParseEmployeeFileFn
            extends DoFn<FileIO.ReadableFile, KV<Long, EmployeeRecord>> {

        private static final Logger LOGGER =
                LoggerFactory.getLogger(ParseEmployeeFileFn.class);

        private transient ObjectMapper objectMapper;
        private transient JsonEmployeeParser parser;

        @Setup
        public void setup() {

            objectMapper = new ObjectMapper();
            parser = new JsonEmployeeParser(objectMapper);

            LOGGER.info(
                    "JSON employee parser initialized"
            );
        }

        @ProcessElement
        public void processElement(ProcessContext context) {

            FileIO.ReadableFile file = context.element();

            String filePath =
                    file.getMetadata().resourceId().toString();

            LOGGER.info(
                    "Starting JSON file processing: {}",
                    filePath
            );

            try {

                String jsonDocument =
                        file.readFullyAsUTF8String();

                String trimmedJson =
                        jsonDocument.trim();

                if (trimmedJson.isEmpty()) {

                    LOGGER.warn(
                            "JSON file is empty: {}",
                            filePath
                    );

                    return;
                }

                /*
                 * Case 1:
                 *
                 * JSON Array
                 *
                 * [
                 *   {...},
                 *   {...}
                 * ]
                 */
                if (trimmedJson.startsWith("[")) {

                    LOGGER.info(
                            "Detected JSON array format for file: {}",
                            filePath
                    );

                    List<EmployeeRecord> employees =
                            parser.parse(trimmedJson);

                    LOGGER.info(
                            "Parsed {} records from JSON array file: {}",
                            employees.size(),
                            filePath
                    );

                    for (int index = 0;
                         index < employees.size();
                         index++) {

                        long recordNumber = index + 1;

                        context.output(
                                KV.of(
                                        recordNumber,
                                        employees.get(index)
                                )
                        );
                    }

                    LOGGER.info(
                            "Completed JSON array file processing. File: {}, Records: {}",
                            filePath,
                            employees.size()
                    );

                    return;
                }

                /*
                 * Case 2:
                 *
                 * JSON Lines / NDJSON
                 *
                 * {"employee_id":"EMP001", ...}
                 * {"employee_id":"EMP002", ...}
                 */
                LOGGER.info(
                        "Detected JSON Lines format for file: {}",
                        filePath
                );

                String[] lines =
                        trimmedJson.split("\\R");

                long recordNumber = 0;

                for (String line : lines) {

                    String trimmedLine =
                            line.trim();

                    if (trimmedLine.isEmpty()) {
                        continue;
                    }

                    recordNumber++;

                    EmployeeRecord employee =
                            parser.parseRecord(trimmedLine);

                    context.output(
                            KV.of(
                                    recordNumber,
                                    employee
                            )
                    );
                }

                LOGGER.info(
                        "Completed JSON Lines file processing. File: {}, Records: {}",
                        filePath,
                        recordNumber
                );

            } catch (Exception exception) {

                LOGGER.error(
                        "Failed to parse JSON file: {}",
                        filePath,
                        exception
                );

                throw new RuntimeException(
                        "Unable to parse employee JSON file: "
                                + filePath,
                        exception
                );
            }
        }
    }
}
