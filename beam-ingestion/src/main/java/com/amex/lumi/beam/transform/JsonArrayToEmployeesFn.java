package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.parser.JsonEmployeeParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Converts one JSON document into employee records for the Beam pipeline.
 */
public class JsonArrayToEmployeesFn
        extends DoFn<String, EmployeeRecord> {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(JsonArrayToEmployeesFn.class);

    private transient ObjectMapper objectMapper;

    private transient JsonEmployeeParser parser;

    @Setup
    public void setup() {

        objectMapper =
                new ObjectMapper();

        parser =
                new JsonEmployeeParser(objectMapper);
    }

    /**
     * Parses the document and emits each employee as an independent element.
     *
     * @param context Beam processing context
     */
    @ProcessElement
    public void processElement(ProcessContext context) {

        String jsonDocument =
                context.element();

        try {

            List<EmployeeRecord> employees =
                    parser.parse(jsonDocument);

            for (EmployeeRecord employee : employees) {
                context.output(employee);
            }

        } catch (Exception exception) {

            LOGGER.error(
                    "Unable to parse employee JSON document",
                    exception
            );

            throw new RuntimeException(
                    "Unable to parse employee JSON document",
                    exception
            );
        }
    }
}
