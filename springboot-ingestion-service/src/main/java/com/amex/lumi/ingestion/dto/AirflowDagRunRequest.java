package com.amex.lumi.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class AirflowDagRunRequest {

    @JsonProperty("dag_run_id")
    private String dagRunId;

    private Map<String, Object> conf;

    public AirflowDagRunRequest(
            String dagRunId,
            Map<String, Object> conf) {

        this.dagRunId = dagRunId;
        this.conf = conf;
    }

    public String getDagRunId() {
        return dagRunId;
    }

    public void setDagRunId(String dagRunId) {
        this.dagRunId = dagRunId;
    }

    public Map<String, Object> getConf() {
        return conf;
    }

    public void setConf(Map<String, Object> conf) {
        this.conf = conf;
    }
}
