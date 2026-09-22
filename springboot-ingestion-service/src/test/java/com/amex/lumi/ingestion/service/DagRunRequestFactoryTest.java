package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.client.AirflowDagRunRequest;
import com.amex.lumi.ingestion.config.IngestionProperties;
import com.amex.lumi.ingestion.model.FileType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;

import static com.amex.lumi.ingestion.client.DagRunConfKeys.ERROR_OUTPUT;
import static com.amex.lumi.ingestion.client.DagRunConfKeys.FILE_TYPE;
import static com.amex.lumi.ingestion.client.DagRunConfKeys.INPUT_FILE;
import static com.amex.lumi.ingestion.client.DagRunConfKeys.REQUIRES_SPLIT;
import static com.amex.lumi.ingestion.client.DagRunConfKeys.SPLIT_OUTPUT_DIR;
import static org.assertj.core.api.Assertions.assertThat;

class DagRunRequestFactoryTest {

    @TempDir
    Path tempDir;

    private DagRunRequestFactory factory() {
        return new DagRunRequestFactory(new IngestionPathResolver(new IngestionProperties(1,
                tempDir.resolve("data").toString(), "/opt/lumi/data",
                tempDir.resolve("control-files").toString(), "/opt/lumi/control-files")));
    }

    @Test
    void buildsRunIdAndContainerPaths() {
        UUID id = UUID.randomUUID();
        AirflowDagRunRequest request = factory().create(new DagRunRequestFactory.RunDetails(id,
                tempDir.resolve("data/samples/employees.json"), tempDir.resolve("control-files/c.properties"),
                FileType.JSON, 500, 1, 20, true));

        assertThat(request.dagRunId()).isEqualTo("ingestion_" + id);
        assertThat(request.conf())
                .containsEntry(INPUT_FILE, "/opt/lumi/data/samples/employees.json")
                .containsEntry(FILE_TYPE, "JSON")
                .containsEntry(REQUIRES_SPLIT, true)
                .containsEntry(SPLIT_OUTPUT_DIR, "/opt/lumi/data/split/" + id)
                .containsEntry(ERROR_OUTPUT, "/opt/lumi/data/error/execution-" + id);
    }

    @Test
    void sendsTenParametersToTheDag() {
        AirflowDagRunRequest request = factory().create(new DagRunRequestFactory.RunDetails(UUID.randomUUID(),
                tempDir.resolve("data/a.csv"), tempDir.resolve("control-files/c.properties"),
                FileType.CSV, 10, 100, 1, false));

        assertThat(request.conf()).hasSize(10).containsEntry(REQUIRES_SPLIT, false);
    }
}
