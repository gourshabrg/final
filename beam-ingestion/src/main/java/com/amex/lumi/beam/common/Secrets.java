package com.amex.lumi.beam.common;

import java.util.function.UnaryOperator;

/**
 * Reads a secret from the option (tests) or the environment variable (hidden from ps).
 */
public final class Secrets {

    public static final String ENCRYPTION_KEY = "LUMI_ENCRYPTION_KEY";
    public static final String WAREHOUSE_PASSWORD = "LUMI_WAREHOUSE_PASSWORD";

    private Secrets() {
    }

    public static String resolve(String optionValue, String environmentVariable) {
        return resolve(optionValue, environmentVariable, System::getenv);
    }

    static String resolve(String optionValue, String environmentVariable, UnaryOperator<String> environment) {
        if (optionValue != null && !optionValue.isBlank()) {
            return optionValue;
        }
        String value = environment.apply(environmentVariable);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Set the " + environmentVariable + " environment variable");
        }
        return value;
    }
}
