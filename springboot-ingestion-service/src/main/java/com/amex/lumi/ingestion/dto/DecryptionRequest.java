package com.amex.lumi.ingestion.dto;

import jakarta.validation.constraints.NotBlank;

public class DecryptionRequest {

    @NotBlank(message = "encryptedValue is required")
    private String encryptedValue;

    public DecryptionRequest() {
    }

    public String getEncryptedValue() {
        return encryptedValue;
    }

    public void setEncryptedValue(String encryptedValue) {
        this.encryptedValue = encryptedValue;
    }
}
