package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.client.AirflowClient;
import com.amex.lumi.ingestion.client.AirflowDagRunRequest;
import com.amex.lumi.ingestion.config.IngestionProperties;
import com.amex.lumi.ingestion.dto.IngestionRequest;
import com.amex.lumi.ingestion.dto.IngestionResponse;
import com.amex.lumi.ingestion.exception.InvalidRequestException;
import com.amex.lumi.ingestion.model.FileType;
import com.amex.lumi.ingestion.repository.IngestionExecutionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class IngestionServiceTest {

    @TempDir
    Path tempDir;

    private final AirflowClient airflowClient = mock(AirflowClient.class);

    @BeforeEach
    void createFiles() throws IOException {
        Files.createDirectories(tempDir.resolve("data/samples"));
        Files.createDirectories(tempDir.resolve("control-files"));
        Files.writeString(tempDir.resolve("data/samples/employees.csv"), "employee_id\nEMP0001\n");
        Files.writeString(tempDir.resolve("control-files/employees.properties"), "record_count=1");
    }

    private IngestionService service(long thresholdBytes) {
        IngestionProperties properties = new IngestionProperties(thresholdBytes,
                tempDir.resolve("data").toString(), "/opt/lumi/data",
                tempDir.resolve("control-files").toString(), "/opt/lumi/control-files");
        IngestionPathResolver paths = new IngestionPathResolver(properties);
        return new IngestionService(paths, new SourceFileValidator(), new ControlFileValidator(),
                new DagRunRequestFactory(paths), airflowClient, mock(IngestionExecutionRepository.class), properties);
    }

    @Test
    void triggersDagWithContainerPathsAndSplitDecision() {
        IngestionResponse response = service(1).startIngestion(
                new IngestionRequest("samples/employees.csv", "employees.properties", FileType.CSV));

        ArgumentCaptor<AirflowDagRunRequest> captor = ArgumentCaptor.forClass(AirflowDagRunRequest.class);
        verify(airflowClient).triggerDag(captor.capture());
        Map<String, Object> conf = captor.getValue().conf();
        String id = response.executionId().toString();

        assertThat(response.status()).isEqualTo("ACCEPTED");
        assertThat(captor.getValue().dagRunId()).isEqualTo("ingestion_" + id);
        assertThat(conf)
                .containsEntry("execution_id", id)
                .containsEntry("input_file", "/opt/lumi/data/samples/employees.csv")
                .containsEntry("control_file", "/opt/lumi/control-files/employees.properties")
                .containsEntry("file_type", "CSV")
                .containsEntry("expected_record_count", 1L)
                .containsEntry("requires_split", true)
                .containsEntry("split_output_dir", "/opt/lumi/data/split/" + id)
                .containsEntry("error_output", "/opt/lumi/data/error/execution-" + id);
    }

    @Test
    void smallFileSkipsSplitWhenBelowThreshold() {
        IngestionResponse response = service(1_000_000).startIngestion(
                new IngestionRequest("samples/employees.csv", "employees.properties", FileType.CSV));

        assertThat(response.requiresSplit()).isFalse();
    }

    @Test
    void invalidRequestNeverReachesAirflow() {
        assertThatThrownBy(() -> service(1).startIngestion(
                new IngestionRequest("samples/employees.csv", "missing.properties", FileType.CSV)))
                .isInstanceOf(InvalidRequestException.class);
        verifyNoInteractions(airflowClient);
    }
}
