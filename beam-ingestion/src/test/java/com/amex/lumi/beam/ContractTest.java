package com.amex.lumi.beam;

import com.amex.lumi.beam.common.PipelineConstants;
import com.amex.lumi.beam.encryption.AesGcmEncryptionService;
import com.amex.lumi.beam.execution.ControlFileReader;
import com.amex.lumi.beam.model.FileType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Compares Beam's constants with contracts/lumi-contract.json (shared by all modules).
 */
class ContractTest {

    private static final JsonNode CONTRACT = read();

    @Test
    void fileTypesMatch() {
        assertEquals(strings(CONTRACT.get("file_types")),
                Arrays.stream(FileType.values()).map(Enum::name).toList());
    }

    @Test
    void splitColumnsMatchWhatPySparkWrites() {
        assertEquals(CONTRACT.at("/split_columns/source_record_number").asText(),
                PipelineConstants.SOURCE_RECORD_NUMBER);
        assertEquals(CONTRACT.at("/split_columns/corrupt_record").asText(), PipelineConstants.CORRUPT_RECORD);
    }

    @Test
    void controlFileKeysMatch() {
        assertEquals(strings(CONTRACT.get("control_file_keys")),
                List.of(ControlFileReader.RECORD_COUNT, ControlFileReader.FILE_NAME, ControlFileReader.SHA256));
    }

    // The API decrypts this same vector, so both encryption codes must agree.
    @Test
    void encryptionFormatMatchesTheApi() {
        JsonNode vector = CONTRACT.get("encryption_v1");
        AesGcmEncryptionService service = AesGcmEncryptionService.fromKey(vector.get("key").asText());

        assertEquals(vector.get("plain_text").asText(), service.decrypt(vector.get("encrypted").asText()));
    }

    private static List<String> strings(JsonNode array) {
        List<String> values = new ArrayList<>();
        array.forEach(value -> values.add(value.asText()));
        return values;
    }

    private static JsonNode read() {
        try {
            return new ObjectMapper().readTree(Path.of("..", "contracts", "lumi-contract.json").toFile());
        } catch (IOException exception) {
            throw new IllegalStateException("contracts/lumi-contract.json not found", exception);
        }
    }
}
