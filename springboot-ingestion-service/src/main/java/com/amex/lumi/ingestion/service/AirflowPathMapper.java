package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.config.IngestionProperties;
import org.springframework.stereotype.Component;

import java.nio.file.Path;

@Component
public class AirflowPathMapper {

    private final IngestionProperties properties;

    public AirflowPathMapper(IngestionProperties properties) {
        this.properties = properties;
    }

    /**
     * Converts a local data file path into
     * the corresponding Airflow container path.
     */
    public String toAirflowPath(String localFilePath) {

        Path localRoot =
                Path.of(properties.getLocalDataRoot())
                        .toAbsolutePath()
                        .normalize();

        Path file =
                Path.of(localFilePath)
                        .toAbsolutePath()
                        .normalize();

        if (!file.startsWith(localRoot)) {
            throw new IllegalArgumentException(
                    "File must be located under configured data root: "
                            + localRoot
            );
        }

        Path relativePath =
                localRoot.relativize(file);

        return properties.getAirflowDataRoot()
                + "/"
                + relativePath.toString().replace('\\', '/');
    }


    /**
     * Converts a local control file path into
     * the corresponding Airflow container path.
     */
    public String toAirflowControlFilePath(String localFilePath) {

        Path localRoot =
                Path.of(properties.getLocalControlFileRoot())
                        .toAbsolutePath()
                        .normalize();

        Path file =
                Path.of(localFilePath)
                        .toAbsolutePath()
                        .normalize();

        if (!file.startsWith(localRoot)) {
            throw new IllegalArgumentException(
                    "Control file must be located under configured "
                            + "control file root: "
                            + localRoot
            );
        }

        Path relativePath =
                localRoot.relativize(file);

        return properties.getAirflowControlFileRoot()
                + "/"
                + relativePath.toString().replace('\\', '/');
    }
}
