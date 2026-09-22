package com.amex.lumi.ingestion.controller;

import com.amex.lumi.ingestion.dto.DecryptionRequest;
import com.amex.lumi.ingestion.dto.DecryptionResponse;
import com.amex.lumi.ingestion.dto.IngestionRequest;
import com.amex.lumi.ingestion.dto.IngestionResponse;
import com.amex.lumi.ingestion.service.DecryptionService;
import com.amex.lumi.ingestion.service.IngestionService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/ingestions")
public class IngestionController {

    private final IngestionService ingestionService;
    private final DecryptionService decryptionService;

    public IngestionController(IngestionService ingestionService, DecryptionService decryptionService) {
        this.ingestionService = ingestionService;
        this.decryptionService = decryptionService;
    }

    @GetMapping("/health")
    public String health() {
        return "Lumi ingestion service is running";
    }

    @PostMapping("/decrypt")
    public ResponseEntity<DecryptionResponse> decryptValue(
            @Valid @RequestBody DecryptionRequest request) {

        String decryptedValue = decryptionService.decrypt(request.getEncryptedValue());
        return ResponseEntity.ok(new DecryptionResponse(decryptedValue, "SUCCESS"));
    }

    @PostMapping
    public ResponseEntity<IngestionResponse> startIngestion(
            @Valid @RequestBody IngestionRequest request) {

        IngestionResponse response =
                ingestionService.startIngestion(request);

        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(response);
    }
}
