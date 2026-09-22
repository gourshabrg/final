package com.amex.lumi.beam.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Command-line and runner options required by the ingestion pipeline.
 */
public interface IngestionPipelineOptions
        extends PipelineOptions {

  @Description("Input employee file location")
String getInputFile();

void setInputFile(String inputFile);

@Description("Unique ingestion execution ID")
String getExecutionId();

void setExecutionId(String executionId);

@Description("Ingestion timestamp for this execution in ISO-8601 format")
String getIngestionTimestamp();

void setIngestionTimestamp(String ingestionTimestamp);

@Description("Source file creation timestamp in ISO-8601 format")
String getSourceCreationTime();

void setSourceCreationTime(String sourceCreationTime);

@Description("Directory/prefix for validation error output")
String getErrorOutput();

void setErrorOutput(String errorOutput);

@Description("AES-256 encryption key for local development")
String getEncryptionKey();

void setEncryptionKey(String encryptionKey);

@Description("PostgreSQL JDBC URL")
String getJdbcUrl();

void setJdbcUrl(String jdbcUrl);

@Description("PostgreSQL username")
String getJdbcUsername();

void setJdbcUsername(String jdbcUsername);

@Description("PostgreSQL password")
String getJdbcPassword();

void setJdbcPassword(String jdbcPassword);

@Description("Ingestion control file location")
String getControlFile();

void setControlFile(String controlFile);

@Description("Input file type: JSON or CSV")
@Default.String("JSON")
String getFileType();

void setFileType(String fileType);


}
