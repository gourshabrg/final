package com.amex.lumi.beam.read;

import com.amex.lumi.beam.common.PipelineConstants;
import com.amex.lumi.beam.model.EmployeeRecord;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.type.LogicalType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.util.stream.Collectors;

/**
 * Reads a JSON array or JSON Lines, one object at a time.
 */
public class JsonEmployeeParser implements EmployeeFileParser {

    private static final long serialVersionUID = 1L;

    static final String INVALID_JSON = "record is not valid JSON";
    static final String NOT_AN_OBJECT = "record is not a JSON object";

    // Thread-safe, so one shared instance is enough.
    private static final ObjectMapper MAPPER = strictMapper();

    // Strict types: salary 950000.99 or "950000" and is_active 1 are rejected.
    private static ObjectMapper strictMapper() {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(DeserializationFeature.ACCEPT_FLOAT_AS_INT, false);
        mapper.coercionConfigFor(LogicalType.Integer)
                .setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.String, CoercionAction.Fail);
        mapper.coercionConfigFor(LogicalType.Boolean)
                .setCoercion(CoercionInputShape.Integer, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.String, CoercionAction.Fail);
        return mapper;
    }

    @Override
    public void parse(Reader reader, RecordHandler handler) throws IOException {
        PushbackReader input = new PushbackReader(reader, 1);
        if (firstNonBlank(input) == '[') {
            parseArray(input, handler);
        } else {
            parseLines(new BufferedReader(input), handler);
        }
    }

    // After a syntax error in an array the next record cannot be found, so the rest is skipped.
    private static void parseArray(Reader reader, RecordHandler handler) throws IOException {
        long read = 0;
        try (JsonParser json = MAPPER.createParser(reader)) {
            json.nextToken();
            JsonToken token = json.nextToken();
            while (token == JsonToken.START_OBJECT) {
                readOne(MAPPER.readTree(json), handler);
                read++;
                token = json.nextToken();
            }
            if (token != JsonToken.END_ARRAY) {
                handler.onFileError("JSON array contains a value that is not an object after record " + read);
            }
        } catch (JsonProcessingException exception) {
            handler.onFileError("file is not valid JSON after record " + read);
        }
    }

    // One record per line: a broken line only rejects that record.
    private static void parseLines(BufferedReader reader, RecordHandler handler) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isBlank()) {
                continue;
            }
            JsonNode node;
            try {
                node = MAPPER.readTree(line);
            } catch (JsonProcessingException exception) {
                handler.onError(INVALID_JSON, null);
                continue;
            }
            if (node.isObject()) {
                readOne(node, handler);
            } else {
                handler.onError(NOT_AN_OBJECT, null);
            }
        }
    }

    private static void readOne(JsonNode node, RecordHandler handler) {
        // Only split files have these fields.
        JsonNode number = node.get(PipelineConstants.SOURCE_RECORD_NUMBER);
        Long sourceRecordNumber = number != null && number.canConvertToLong() ? number.asLong() : null;
        if (node.hasNonNull(PipelineConstants.CORRUPT_RECORD)) {
            handler.onError(INVALID_JSON, sourceRecordNumber);
            return;
        }
        try {
            handler.onRecord(MAPPER.treeToValue(node, EmployeeRecord.class), sourceRecordNumber);
        } catch (JsonProcessingException exception) {
            handler.onError(describe(exception), sourceRecordNumber);
        }
    }

    private static int firstNonBlank(PushbackReader reader) throws IOException {
        int next = reader.read();
        while (next != -1 && Character.isWhitespace(next)) {
            next = reader.read();
        }
        if (next != -1) {
            reader.unread(next);
        }
        return next;
    }

    // Report only the field name; Jackson's message would include the value.
    private static String describe(JsonProcessingException exception) {
        if (!(exception instanceof JsonMappingException mapping)) {
            return INVALID_JSON;
        }
        String field = mapping.getPath().stream()
                .map(reference -> reference.getFieldName() != null
                        ? reference.getFieldName()
                        : "[" + reference.getIndex() + "]")
                .collect(Collectors.joining("."));
        return field.isEmpty() ? "record has an invalid structure" : "invalid value for field '" + field + "'";
    }
}
