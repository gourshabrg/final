package com.amex.lumi.ingestion.client;

import com.amex.lumi.ingestion.config.AirflowProperties;
import com.amex.lumi.ingestion.dto.AirflowDagRunRequest;
import com.amex.lumi.ingestion.exception.AirflowTriggerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class AirflowRestClient implements AirflowClient {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(AirflowRestClient.class);

    private final RestClient restClient;
    private final AirflowProperties properties;

    public AirflowRestClient(
            AirflowProperties properties) {

        this.properties = properties;

        this.restClient = RestClient.builder()
                .baseUrl(properties.getBaseUrl())
                .defaultHeaders(headers ->
                        headers.setBasicAuth(
                                properties.getUsername(),
                                properties.getPassword()
                        )
                )
                .build();
    }

    @Override
    public void triggerDag(
            AirflowDagRunRequest request) {

        try {

            restClient.post()
                    .uri(
                            "/api/v1/dags/{dagId}/dagRuns",
                            properties.getDagId()
                    )
                    .contentType(
                            MediaType.APPLICATION_JSON
                    )
                    .body(request)
                    .retrieve()
                    .toBodilessEntity();

            LOGGER.info(
                    "Airflow DAG triggered successfully: dagId={}, dagRunId={}",
                    properties.getDagId(),
                    request.getDagRunId()
            );

        } catch (Exception exception) {

            LOGGER.error(
                    "Failed to trigger Airflow DAG: dagId={}, dagRunId={}",
                    properties.getDagId(),
                    request.getDagRunId(),
                    exception
            );

            throw new AirflowTriggerException(
                    "Unable to trigger Airflow DAG",
                    exception
            );
        }
    }
}

