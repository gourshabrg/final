package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.config.IngestionProperties;
import com.amex.lumi.ingestion.exception.InvalidRequestException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IngestionPathResolverTest {

    @TempDir
    Path tempDir;

    private IngestionPathResolver resolver() {
        return new IngestionPathResolver(new IngestionProperties(1,
                tempDir.resolve("data").toString(), "/opt/lumi/data",
                tempDir.resolve("control-files").toString(), "/opt/lumi/control-files"));
    }

    @Test
    void relativePathIsResolvedUnderDataRootAndMappedForAirflow() {
        IngestionPathResolver resolver = resolver();
        Path file = resolver.resolveDataFile("samples/employees.csv");

        assertThat(file).isEqualTo(tempDir.resolve("data/samples/employees.csv").toAbsolutePath());
        assertThat(resolver.toAirflowDataPath(file)).isEqualTo("/opt/lumi/data/samples/employees.csv");
    }

    @Test
    void absolutePathInsideRootIsAccepted() {
        Path absolute = tempDir.resolve("control-files/employees.properties").toAbsolutePath();

        assertThat(resolver().toAirflowControlPath(resolver().resolveControlFile(absolute.toString())))
                .isEqualTo("/opt/lumi/control-files/employees.properties");
    }

    @Test
    void pathEscapingTheRootIsRejected() {
        assertThatThrownBy(() -> resolver().resolveDataFile("../secrets.csv"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessageContaining("must be inside");
    }

    @Test
    void buildsSplitAndErrorPathsForAirflow() {
        IngestionPathResolver resolver = resolver();

        assertThat(resolver.airflowSplitDirectory("abc")).isEqualTo("/opt/lumi/data/split/abc");
        assertThat(resolver.airflowErrorOutput("abc")).isEqualTo("/opt/lumi/data/error/execution-abc");
    }

    @Test
    void controlFileOutsideItsRootIsRejected() {
        assertThatThrownBy(() -> resolver().resolveControlFile("../data/samples/employees.csv"))
                .isInstanceOf(InvalidRequestException.class);
    }
}
