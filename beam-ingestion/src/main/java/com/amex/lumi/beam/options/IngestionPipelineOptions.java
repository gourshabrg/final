package com.amex.lumi.beam.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Command-line arguments of the Beam job, e.g. --inputFile=... --fileType=CSV.
 */
public interface IngestionPipelineOptions extends PipelineOptions {

    @Description("Input file or glob pattern, e.g. /opt/lumi/data/split/<id>/*.csv")
    @Validation.Required
    String getInputFile();

    void setInputFile(String value);

    @Description("CSV or JSON")
    @Validation.Required
    String getFileType();

    void setFileType(String value);

    @Description("Execution ID (UUID) created by the Spring Boot API")
    @Validation.Required
    String getExecutionId();

    void setExecutionId(String value);

    @Description("Control file with record_count")
    @Validation.Required
    String getControlFile();

    void setControlFile(String value);

    @Description("Error file prefix; .txt is added")
    @Validation.Required
    String getErrorOutput();

    void setErrorOutput(String value);

    @Description("32-character AES-256 key")
    @Validation.Required
    String getEncryptionKey();

    void setEncryptionKey(String value);

    @Description("Warehouse JDBC URL")
    @Validation.Required
    String getJdbcUrl();

    void setJdbcUrl(String value);

    @Description("Warehouse user")
    @Validation.Required
    String getJdbcUsername();

    void setJdbcUsername(String value);

    @Description("Warehouse password")
    @Validation.Required
    String getJdbcPassword();

    void setJdbcPassword(String value);
}
