package com.amex.lumi.ingestion.client;

/**
 * Starts DAG runs. An interface so tests can use a mock instead of a real Airflow.
 */
public interface AirflowClient {

    void triggerDag(AirflowDagRunRequest request);
}
