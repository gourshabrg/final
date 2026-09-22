package com.amex.lumi.ingestion.client;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Body of Airflow's POST /api/v1/dags/{dagId}/dagRuns. "conf" becomes dag_run.conf in the DAG.
 */
public record AirflowDagRunRequest(
        @JsonProperty("dag_run_id") String dagRunId,
        Map<String, Object> conf) {
}
