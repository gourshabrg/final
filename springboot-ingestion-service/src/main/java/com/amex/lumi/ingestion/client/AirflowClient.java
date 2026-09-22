package com.amex.lumi.ingestion.client;

import com.amex.lumi.ingestion.dto.AirflowDagRunRequest;

public interface AirflowClient {

    void triggerDag(
            AirflowDagRunRequest request
    );
}
