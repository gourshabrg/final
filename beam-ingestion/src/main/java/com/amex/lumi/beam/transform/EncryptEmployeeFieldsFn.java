package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.encryption.AesGcmEncryptionService;
import com.amex.lumi.beam.encryption.EncryptionService;
import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.model.EncryptedEmployeeRecord;
import com.amex.lumi.beam.model.EmergencyContact;
import com.amex.lumi.beam.model.EnrichedEmployeeRecord;

import org.apache.beam.sdk.transforms.DoFn;

/**
 * Encrypts sensitive employee fields before they are persisted.
 */
public class EncryptEmployeeFieldsFn
        extends DoFn<EnrichedEmployeeRecord, EncryptedEmployeeRecord> {

    private final String encryptionKey;

    private transient EncryptionService encryptionService;

    public EncryptEmployeeFieldsFn(String encryptionKey) {
        this.encryptionKey = encryptionKey;
    }

    @Setup
    public void setup() {

        encryptionService =
                new AesGcmEncryptionService(
                        encryptionKey.getBytes(
                                java.nio.charset.StandardCharsets.UTF_8
                        )
                );
    }

    @ProcessElement
    public void processElement(ProcessContext context) {

        EnrichedEmployeeRecord enriched =
                context.element();

        EmployeeRecord employee =
                enriched.getEmployee();

        String encryptedPhoneNumber =
                encryptPhoneNumber(employee);

        String encryptedSalary =
                encryptSalary(employee);

        String encryptedEmergencyPhone =
                encryptEmergencyContactPhone(employee);

        EncryptedEmployeeRecord encrypted =
        new EncryptedEmployeeRecord(
                employee,
                encryptedPhoneNumber,
                encryptedSalary,
                encryptedEmergencyPhone,
                enriched.getRecordNumber(),
                enriched.getSourceFile(),
                enriched.getExecutionId(),
                enriched.getIngestionTimestamp(),
                enriched.getSourceCreationTime()
        );


        context.output(encrypted);
    }

    private String encryptPhoneNumber(
            EmployeeRecord employee) {

        if (employee.getPhoneNumber() == null) {
            return null;
        }

        return encryptionService.encrypt(
                employee.getPhoneNumber()
        );
    }

    private String encryptSalary(
            EmployeeRecord employee) {

        if (employee.getSalary() == null) {
            return null;
        }

        return encryptionService.encrypt(
                String.valueOf(employee.getSalary())
        );
    }

    private String encryptEmergencyContactPhone(
            EmployeeRecord employee) {

        EmergencyContact emergencyContact =
                employee.getEmergencyContact();

        if (emergencyContact == null
                || emergencyContact.getPhone() == null) {

            return null;
        }

        return encryptionService.encrypt(
                emergencyContact.getPhone()
        );
    }
}
