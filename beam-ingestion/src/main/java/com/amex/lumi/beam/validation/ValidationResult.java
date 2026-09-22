package com.amex.lumi.beam.validation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Immutable validation outcome containing validity and field-level errors.
 */
public class ValidationResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean valid;

    private final List<String> errors;

    private ValidationResult(
            boolean valid,
            List<String> errors) {

        this.valid = valid;
        this.errors = errors;
    }

    /**
     * Creates a successful validation outcome.
     *
     * @return valid result with no errors
     */
    public static ValidationResult valid() {

        return new ValidationResult(
                true,
                new ArrayList<>()
        );
    }

    /**
     * Creates a failed validation outcome with a defensive error copy.
     *
     * @param errors field-level validation messages
     * @return invalid result
     */
    public static ValidationResult invalid(
            List<String> errors) {

        return new ValidationResult(
                false,
                new ArrayList<>(errors)
        );
    }

    public boolean isValid() {
        return valid;
    }

    public List<String> getErrors() {
        return errors;
    }
}
