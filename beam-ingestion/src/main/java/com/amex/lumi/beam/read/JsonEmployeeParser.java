package com.amex.lumi.beam.read;

import com.amex.lumi.beam.model.EmployeeRecord;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Reader;
import java.util.stream.Collectors;

/**
 * Reads JSON files: either an array [ {...}, {...} ] or one object per line (what PySpark writes).
 * Objects are read one at a time, so a big file does not fill the memory.
 */
public class JsonEmployeeParser implements EmployeeFileParser {

    private static final long serialVersionUID = 1L;

    // Thread-safe, so one shared instance is enough.
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public void parse(Reader reader, RecordHandler handler) throws IOException {
        try (JsonParser json = MAPPER.createParser(reader)) {
            JsonToken token = json.nextToken();
            if (token == JsonToken.START_ARRAY) {
                token = json.nextToken();
                while (token == JsonToken.START_OBJECT) {
                    readOne(json, handler);
                    token = json.nextToken();
                }
                return;
            }
            // JSON Lines: one object per line.
            while (token == JsonToken.START_OBJECT) {
                readOne(json, handler);
                token = json.nextToken();
            }
            if (token != null) {
                throw new IOException("Expected a JSON array or JSON objects but found " + token);
            }
        }
    }

    private static void readOne(JsonParser json, RecordHandler handler) throws IOException {
        JsonNode node = MAPPER.readTree(json);
        try {
            handler.onRecord(MAPPER.treeToValue(node, EmployeeRecord.class));
        } catch (JsonMappingException exception) {
            handler.onError(describe(exception));
        }
    }

    // Report only the field name; Jackson's message would include the value.
    private static String describe(JsonMappingException exception) {
        String field = exception.getPath().stream()
                .map(reference -> reference.getFieldName() != null
                        ? reference.getFieldName()
                        : "[" + reference.getIndex() + "]")
                .collect(Collectors.joining("."));
        return field.isEmpty() ? "record has an invalid structure" : "invalid value for field '" + field + "'";
    }
}
