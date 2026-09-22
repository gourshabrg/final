package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.dto.IngestionRequest;
import com.amex.lumi.ingestion.dto.IngestionResponse;

public interface IngestionService {

    IngestionResponse startIngestion(IngestionRequest request);
}
