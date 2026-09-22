package com.amex.lumi.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

import java.util.TimeZone;

/**
 * REST API that validates ingestion requests and triggers the Airflow DAG.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class IngestionServiceApplication {

    public static void main(String[] args) {
        // Same time zone as the database and the Beam job.
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SpringApplication.run(IngestionServiceApplication.class, args);
    }
}
