package com.amex.lumi.ingestion;

import com.amex.lumi.ingestion.client.DagRunConfKeys;
import com.amex.lumi.ingestion.config.EncryptionProperties;
import com.amex.lumi.ingestion.model.FileType;
import com.amex.lumi.ingestion.security.FieldDecryptor;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compares the API's constants with contracts/lumi-contract.json (shared by all modules).
 */
class ContractTest {

    private static final JsonNode CONTRACT = JsonMapper.builder().build()
            .readTree(Path.of("..", "contracts", "lumi-contract.json").toFile());

    @Test
    void fileTypesMatch() {
        assertThat(Arrays.stream(FileType.values()).map(Enum::name).toList())
                .isEqualTo(strings(CONTRACT.get("file_types")));
    }

    @Test
    void dagParametersMatchWhatTheDagReads() throws IllegalAccessException {
        List<String> keys = new ArrayList<>();
        for (Field field : DagRunConfKeys.class.getFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
                keys.add((String) field.get(null));
            }
        }

        assertThat(keys).containsExactlyInAnyOrderElementsOf(strings(CONTRACT.at("/dag_run_conf/all")));
    }

    // Beam encrypted this vector; decrypting it here proves both encryption codes still agree.
    @Test
    void decryptsTheValueBeamEncrypted() {
        JsonNode vector = CONTRACT.get("encryption_v1");
        FieldDecryptor decryptor = new FieldDecryptor(new EncryptionProperties(vector.get("key").asString()));

        assertThat(decryptor.decrypt(vector.get("encrypted").asString()))
                .isEqualTo(vector.get("plain_text").asString());
    }

    private static List<String> strings(JsonNode array) {
        List<String> values = new ArrayList<>();
        array.forEach(value -> values.add(value.asString()));
        return values;
    }
}
