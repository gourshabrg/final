package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.encryption.AesGcmEncryptionService;
import com.amex.lumi.beam.encryption.EncryptionService;
import com.amex.lumi.beam.model.EmergencyContact;
import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.model.EncryptedEmployee;
import com.amex.lumi.beam.model.EnrichedEmployee;
import com.amex.lumi.beam.model.ParsedEmployee;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Prepares valid employees for saving: fill missing values -> add metadata columns -> encrypt sensitive fields.
 */
public class PrepareForWarehouse extends PTransform<PCollection<ParsedEmployee>, PCollection<EncryptedEmployee>> {

    private final String executionId;
    private final String encryptionKey;

    public PrepareForWarehouse(String executionId, String encryptionKey) {
        this.executionId = executionId;
        this.encryptionKey = encryptionKey;
    }

    @Override
    public PCollection<EncryptedEmployee> expand(PCollection<ParsedEmployee> valid) {
        return valid
                .apply("ReplaceMissingValues", ParDo.of(new ReplaceMissingValuesFn()))
                .apply("AddMetadataColumns", ParDo.of(new AddMetadataFn(executionId)))
                .apply("EncryptSensitiveFields", ParDo.of(new EncryptFieldsFn(encryptionKey)));
    }

    static class ReplaceMissingValuesFn extends DoFn<ParsedEmployee, ParsedEmployee> {
        @ProcessElement
        public void processElement(@Element ParsedEmployee record, OutputReceiver<ParsedEmployee> out) {
            out.output(new ParsedEmployee(record.recordNumber(), record.sourceFile(),
                    record.sourceCreationTime(), MissingValueCleanser.cleanse(record.employee())));
        }
    }

    static class AddMetadataFn extends DoFn<ParsedEmployee, EnrichedEmployee> {

        private final String executionId;

        AddMetadataFn(String executionId) {
            this.executionId = executionId;
        }

        @ProcessElement
        public void processElement(@Element ParsedEmployee record, OutputReceiver<EnrichedEmployee> out) {
            out.output(new EnrichedEmployee(record, executionId, Instant.now()));
        }
    }

    static class EncryptFieldsFn extends DoFn<EnrichedEmployee, EncryptedEmployee> {

        private static final Logger LOGGER = LoggerFactory.getLogger(EncryptFieldsFn.class);

        private final String encryptionKey;
        // The cipher cannot be copied to workers, so each worker creates its own in @Setup.
        private transient EncryptionService encryption;

        EncryptFieldsFn(String encryptionKey) {
            this.encryptionKey = encryptionKey;
        }

        @Setup
        public void setup() {
            try {
                encryption = AesGcmEncryptionService.fromKey(encryptionKey);
            } catch (IllegalArgumentException exception) {
                LOGGER.error("Encryption key is not valid: {}", exception.getMessage());
                throw exception;
            }
        }

        @ProcessElement
        public void processElement(@Element EnrichedEmployee record, OutputReceiver<EncryptedEmployee> out) {
            EmployeeRecord employee = record.employee();
            EmergencyContact contact = employee.getEmergencyContact();
            out.output(new EncryptedEmployee(
                    record,
                    encryptIfPresent(employee.getPhoneNumber()),
                    employee.getSalary() == null ? null : encryption.encrypt(employee.getSalary().toString()),
                    contact == null ? null : encryptIfPresent(contact.getPhone())));
        }

        // A blank placeholder has no data to encrypt.
        private String encryptIfPresent(String value) {
            return value == null || value.isBlank() ? null : encryption.encrypt(value);
        }
    }
}
