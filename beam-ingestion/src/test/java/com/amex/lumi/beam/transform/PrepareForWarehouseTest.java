package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.TestEmployees;
import com.amex.lumi.beam.encryption.AesGcmEncryptionService;
import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.model.EncryptedEmployee;
import com.amex.lumi.beam.model.ParsedEmployee;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrepareForWarehouseTest {

    private static final String KEY = "0123456789abcdef0123456789abcdef";
    private static final String EXECUTION_ID = "22222222-2222-2222-2222-222222222222";
    private static final Instant PARSED_AT = Instant.parse("2026-01-01T10:00:00Z");

    @Test
    void fillsMissingValuesAddsMetadataAndEncrypts() {
        EmployeeRecord employee = TestEmployees.valid();
        employee.setLastName(null);
        employee.setSalary(null);

        Pipeline pipeline = Pipeline.create();
        PCollection<EncryptedEmployee> rows = pipeline
                .apply(Create.of(new ParsedEmployee(1, "in.csv", PARSED_AT, employee)))
                .apply(new PrepareForWarehouse(EXECUTION_ID, KEY));

        PAssert.thatSingleton(rows).satisfies(row -> {
            AesGcmEncryptionService cipher = AesGcmEncryptionService.fromKey(KEY);
            assertEquals(" ", row.employee().getLastName());
            assertEquals(EXECUTION_ID, row.executionId());
            assertEquals(PARSED_AT, row.enriched().sourceCreationTime());
            assertTrue(row.enriched().ingestionTimestamp().isAfter(PARSED_AT));
            assertNotEquals("9876543210", row.encryptedPhoneNumber());
            assertEquals("9876543210", cipher.decrypt(row.encryptedPhoneNumber()));
            assertEquals("9876543211", cipher.decrypt(row.encryptedEmergencyContactPhone()));
            assertNull(row.encryptedSalary());
            return null;
        });

        pipeline.run().waitUntilFinish();
    }
}
