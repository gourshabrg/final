package com.amex.lumi.ingestion.client;

import com.amex.lumi.ingestion.config.AirflowProperties;
import com.amex.lumi.ingestion.exception.AirflowTriggerException;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Uses a tiny local HTTP server in place of Airflow. */
class AirflowRestClientTest {

    private HttpServer server;
    private final AtomicReference<String> receivedPath = new AtomicReference<>();
    private final AtomicReference<String> receivedBody = new AtomicReference<>();
    private final AtomicReference<String> receivedAuth = new AtomicReference<>();

    private AirflowRestClient clientReturning(int status) throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", exchange -> {
            receivedPath.set(exchange.getRequestURI().getPath());
            receivedAuth.set(exchange.getRequestHeaders().getFirst("Authorization"));
            receivedBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        });
        server.start();
        return new AirflowRestClient(new AirflowProperties(
                "http://localhost:" + server.getAddress().getPort(), "airflow", "airflow", "my_dag", 2, 2));
    }

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void postsDagRunWithBasicAuth() throws IOException {
        clientReturning(200).triggerDag(new AirflowDagRunRequest("ingestion_1", Map.of("execution_id", "1")));

        assertThat(receivedPath.get()).isEqualTo("/api/v1/dags/my_dag/dagRuns");
        assertThat(receivedAuth.get()).startsWith("Basic ");
        assertThat(receivedBody.get()).contains("\"dag_run_id\":\"ingestion_1\"").contains("\"execution_id\":\"1\"");
    }

    @Test
    void errorFromAirflowBecomesAirflowTriggerException() throws IOException {
        AirflowRestClient client = clientReturning(409);

        assertThatThrownBy(() -> client.triggerDag(new AirflowDagRunRequest("ingestion_1", Map.of())))
                .isInstanceOf(AirflowTriggerException.class);
    }

    @Test
    void airflowNotRunningBecomesAirflowTriggerException() {
        AirflowRestClient client = new AirflowRestClient(
                new AirflowProperties("http://localhost:1", "airflow", "airflow", "my_dag", 1, 1));

        assertThatThrownBy(() -> client.triggerDag(new AirflowDagRunRequest("ingestion_1", Map.of())))
                .isInstanceOf(AirflowTriggerException.class);
    }
}
