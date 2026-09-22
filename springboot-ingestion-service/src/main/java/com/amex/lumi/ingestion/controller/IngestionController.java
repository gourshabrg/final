package com.amex.lumi.ingestion.controller;

import com.amex.lumi.ingestion.dto.IngestionRequest;
import com.amex.lumi.ingestion.dto.IngestionResponse;
import com.amex.lumi.ingestion.dto.IngestionStatusResponse;
import com.amex.lumi.ingestion.service.IngestionService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/ingestions")
public class IngestionController {

    private final IngestionService ingestionService;

    public IngestionController(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    /** Starts an ingestion. 202 because the work continues in Airflow after we reply. */
    @PostMapping
    public ResponseEntity<IngestionResponse> startIngestion(@Valid @RequestBody IngestionRequest request) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(ingestionService.startIngestion(request));
    }

    /** Status and record counts of one run. */
    @GetMapping("/{executionId}")
    public IngestionStatusResponse getStatus(@PathVariable UUID executionId) {
        return ingestionService.getStatus(executionId);
    }
}
