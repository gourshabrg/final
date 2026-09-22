package com.amex.lumi.beam.validation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RecordCountValidatorTest {

    private final RecordCountValidator validator =
            new RecordCountValidator();

    @Test
    void shouldPassWhenCountsMatch() {

        assertDoesNotThrow(
                () -> validator.validate(10, 10)
        );
    }

    @Test
    void shouldFailWhenCountsDoNotMatch() {

        IllegalStateException exception =
                assertThrows(
                        IllegalStateException.class,
                        () -> validator.validate(10, 9)
                );

        assertEquals(
                "Record count mismatch: expected=10, actual=9",
                exception.getMessage()
        );
    }

    @Test
    void shouldHandleZeroRecords() {

        assertDoesNotThrow(
                () -> validator.validate(0, 0)
        );
    }

    @Test
    void shouldFailWhenExpectedIsZeroButActualIsPositive() {

        assertThrows(
                IllegalStateException.class,
                () -> validator.validate(0, 1)
        );
    }
}
