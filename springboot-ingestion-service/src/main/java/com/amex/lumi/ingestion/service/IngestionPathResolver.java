package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.config.IngestionProperties;
import com.amex.lumi.ingestion.exception.InvalidRequestException;
import org.springframework.stereotype.Component;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;

/**
 * Turns request paths into local paths and into the matching paths inside the Airflow containers.
 * Example: data/samples/employees.csv -> /opt/lumi/data/samples/employees.csv
 */
@Component
public class IngestionPathResolver {

    private static final String SPLIT_FOLDER = "/split/";
    private static final String ERROR_FILE_PREFIX = "/error/execution-";

    private final Path dataRoot;
    private final Path controlFileRoot;
    private final String airflowDataRoot;
    private final String airflowControlFileRoot;

    public IngestionPathResolver(IngestionProperties properties) {
        this.dataRoot = Path.of(properties.localDataRoot()).toAbsolutePath().normalize();
        this.controlFileRoot = Path.of(properties.localControlFileRoot()).toAbsolutePath().normalize();
        this.airflowDataRoot = properties.airflowDataRoot();
        this.airflowControlFileRoot = properties.airflowControlFileRoot();
    }

    public Path resolveDataFile(String location) {
        return resolveUnder(dataRoot, location, "fileLocation");
    }

    public Path resolveControlFile(String location) {
        return resolveUnder(controlFileRoot, location, "controlFileLocation");
    }

    public String toAirflowDataPath(Path localFile) {
        return toAirflowPath(dataRoot, localFile, airflowDataRoot);
    }

    public String toAirflowControlPath(Path localFile) {
        return toAirflowPath(controlFileRoot, localFile, airflowControlFileRoot);
    }

    /** Folder where PySpark writes split files for this run. */
    public String airflowSplitDirectory(String executionId) {
        return airflowDataRoot + SPLIT_FOLDER + executionId;
    }

    /** Error file prefix for this run; Beam adds ".txt". */
    public String airflowErrorOutput(String executionId) {
        return airflowDataRoot + ERROR_FILE_PREFIX + executionId;
    }

    // Airflow can only see these folders, so paths outside them (or "../" tricks) are rejected.
    private static Path resolveUnder(Path root, String location, String fieldName) {
        Path path;
        try {
            path = Path.of(location.trim());
        } catch (InvalidPathException exception) {
            throw new InvalidRequestException(fieldName + " is not a valid path: " + location);
        }
        Path resolved = (path.isAbsolute() ? path : root.resolve(path)).toAbsolutePath().normalize();
        if (!resolved.startsWith(root)) {
            throw new InvalidRequestException(fieldName + " must be inside " + root);
        }
        return resolved;
    }

    private static String toAirflowPath(Path root, Path localFile, String airflowRoot) {
        String relative = root.relativize(localFile).toString().replace('\\', '/');
        return airflowRoot + "/" + relative;
    }
}
