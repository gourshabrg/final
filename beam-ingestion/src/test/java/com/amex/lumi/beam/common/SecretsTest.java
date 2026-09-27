package com.amex.lumi.beam.common;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SecretsTest {

    private final Map<String, String> environment = Map.of(Secrets.ENCRYPTION_KEY, "from-env");

    @Test
    void optionWinsOverTheEnvironment() {
        assertEquals("from-option", Secrets.resolve("from-option", Secrets.ENCRYPTION_KEY, environment::get));
    }

    @Test
    void environmentIsUsedWhenNoOptionIsGiven() {
        assertEquals("from-env", Secrets.resolve(null, Secrets.ENCRYPTION_KEY, environment::get));
    }

    @Test
    void missingSecretSaysWhatToSet() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> Secrets.resolve(" ", Secrets.WAREHOUSE_PASSWORD, environment::get));
        assertEquals("Set the LUMI_WAREHOUSE_PASSWORD environment variable", error.getMessage());
    }
}
