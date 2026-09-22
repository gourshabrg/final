package com.amex.lumi.ingestion.dto;

public class DecryptionResponse {

    private final String decryptedValue;
    private final String status;

    public DecryptionResponse(String decryptedValue, String status) {
        this.decryptedValue = decryptedValue;
        this.status = status;
    }

    public String getDecryptedValue() {
        return decryptedValue;
    }

    public String getStatus() {
        return status;
    }
}
