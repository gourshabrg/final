package com.amex.lumi.ingestion.dto;

import java.util.UUID;

public class IngestionResponse {

    private UUID executionId;

    private String status;

    private String message;

    public IngestionResponse() {
    }

    public IngestionResponse(
            UUID executionId,
            String status,
            String message) {

        this.executionId = executionId;
        this.status = status;
        this.message = message;
    }

    public UUID getExecutionId() {
        return executionId;
    }

    public void setExecutionId(UUID executionId) {
        this.executionId = executionId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
