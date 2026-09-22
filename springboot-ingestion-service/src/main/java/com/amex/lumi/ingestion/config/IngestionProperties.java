package com.amex.lumi.ingestion.config;

import com.amex.lumi.ingestion.enums.FileType;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "lumi.ingestion")
public class IngestionProperties {

    private long fileSizeThresholdBytes;

    private List<FileType> allowedFileTypes = new ArrayList<>();

    private String localDataRoot;

    private String airflowDataRoot;

    private String localControlFileRoot;

private String airflowControlFileRoot;


    public IngestionProperties() {
    }

    public long getFileSizeThresholdBytes() {
        return fileSizeThresholdBytes;
    }

    
public String getLocalControlFileRoot() {
    return localControlFileRoot;
}

public void setLocalControlFileRoot(String localControlFileRoot) {
    this.localControlFileRoot = localControlFileRoot;
}

public String getAirflowControlFileRoot() {
    return airflowControlFileRoot;
}

public void setAirflowControlFileRoot(String airflowControlFileRoot) {
    this.airflowControlFileRoot = airflowControlFileRoot;
}


    public void setFileSizeThresholdBytes(long fileSizeThresholdBytes) {
        this.fileSizeThresholdBytes = fileSizeThresholdBytes;
    }

    public List<FileType> getAllowedFileTypes() {
        return allowedFileTypes;
    }

    public void setAllowedFileTypes(List<FileType> allowedFileTypes) {
        this.allowedFileTypes = allowedFileTypes;
    }

    public String getLocalDataRoot() {
        return localDataRoot;
    }

    public void setLocalDataRoot(String localDataRoot) {
        this.localDataRoot = localDataRoot;
    }

    public String getAirflowDataRoot() {
        return airflowDataRoot;
    }

    public void setAirflowDataRoot(String airflowDataRoot) {
        this.airflowDataRoot = airflowDataRoot;
    }
}
