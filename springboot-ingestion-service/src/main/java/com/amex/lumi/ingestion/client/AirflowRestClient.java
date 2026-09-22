package com.amex.lumi.ingestion.client;

import com.amex.lumi.ingestion.config.AirflowProperties;
import com.amex.lumi.ingestion.exception.AirflowTriggerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.time.Duration;

/**
 * Calls the Airflow REST API with basic auth.
 */
@Component
public class AirflowRestClient implements AirflowClient {

    private static final String DAG_RUNS_PATH = "/api/v1/dags/{dagId}/dagRuns";

    private static final Logger LOGGER = LoggerFactory.getLogger(AirflowRestClient.class);

    private final RestClient restClient;
    private final String dagId;

    public AirflowRestClient(AirflowProperties properties) {
        // Timeouts: if Airflow hangs, our API threads are not blocked forever.
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofSeconds(properties.connectTimeoutSeconds()));
        requestFactory.setReadTimeout(Duration.ofSeconds(properties.readTimeoutSeconds()));

        this.dagId = properties.dagId();
        this.restClient = RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(requestFactory)
                .defaultHeaders(headers -> headers.setBasicAuth(properties.username(), properties.password()))
                .build();
    }

    @Override
    public void triggerDag(AirflowDagRunRequest request) {
        LOGGER.info("Triggering Airflow DAG {} with run id {}", dagId, request.dagRunId());
        try {
            restClient.post()
                    .uri(DAG_RUNS_PATH, dagId)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(request)
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientException exception) {
            LOGGER.error("Airflow rejected or did not answer DAG run {}", request.dagRunId(), exception);
            throw new AirflowTriggerException("Unable to trigger Airflow DAG " + dagId + ": "
                    + exception.getMessage(), exception);
        }
        LOGGER.info("Airflow accepted DAG run {}", request.dagRunId());
    }
}
