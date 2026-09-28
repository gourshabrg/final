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

    @Description("File the user sent. Set when --inputFile points to PySpark split files, "
            + "so errors and the run status name the original file. Defaults to --inputFile")
    String getOriginalFile();

    void setOriginalFile(String value);

    @Description("record_count the API read from the control file; the run fails if the control file changed")
    Long getExpectedRecordCount();

    void setExpectedRecordCount(Long value);

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

    // Secrets: prefer the environment variable, so the value is not visible in the process list.
    @Description("AES-256 key; defaults to the LUMI_ENCRYPTION_KEY environment variable")
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

    @Description("Warehouse password; defaults to the LUMI_WAREHOUSE_PASSWORD environment variable")
    String getJdbcPassword();

    void setJdbcPassword(String value);
}
