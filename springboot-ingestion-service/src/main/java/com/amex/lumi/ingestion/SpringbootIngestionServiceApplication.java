package com.amex.lumi.ingestion;

import com.amex.lumi.ingestion.config.AirflowProperties;
import com.amex.lumi.ingestion.config.IngestionProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.util.TimeZone;

@SpringBootApplication
@EnableConfigurationProperties({
        IngestionProperties.class,
        AirflowProperties.class
})
public class SpringbootIngestionServiceApplication {

    static {
        System.setProperty("user.timezone", "UTC");
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    public static void main(String[] args) {
        SpringApplication.run(
                SpringbootIngestionServiceApplication.class,
                args
        );
    }
}
