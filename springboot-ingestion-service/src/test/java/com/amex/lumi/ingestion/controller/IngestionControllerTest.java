package com.amex.lumi.ingestion.controller;

import com.amex.lumi.ingestion.dto.IngestionResponse;
import com.amex.lumi.ingestion.dto.IngestionStatusResponse;
import com.amex.lumi.ingestion.exception.AirflowTriggerException;
import com.amex.lumi.ingestion.exception.InvalidRequestException;
import com.amex.lumi.ingestion.exception.ResourceNotFoundException;
import com.amex.lumi.ingestion.service.IngestionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(IngestionController.class)
class IngestionControllerTest {

    private static final String VALID_BODY = """
            {"fileLocation":"samples/employees.csv","controlFileLocation":"employees.properties","fileType":"CSV"}
            """;

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private IngestionService ingestionService;

    @Test
    void returns202WithExecutionId() throws Exception {
        UUID id = UUID.randomUUID();
        when(ingestionService.startIngestion(any()))
                .thenReturn(new IngestionResponse(id, "ingestion_" + id, true, "ACCEPTED", "started"));

        mockMvc.perform(post("/api/v1/ingestions").contentType(MediaType.APPLICATION_JSON).content(VALID_BODY))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.executionId").value(id.toString()));
    }

    @Test
    void missingFieldsReturn400() throws Exception {
        mockMvc.perform(post("/api/v1/ingestions").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").exists());
    }

    @Test
    void unsupportedFileTypeReturns400() throws Exception {
        mockMvc.perform(post("/api/v1/ingestions").contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY.replace("CSV", "XML")))
                .andExpect(status().isBadRequest());
    }

    @Test
    void invalidFileReturns400WithReason() throws Exception {
        when(ingestionService.startIngestion(any())).thenThrow(new InvalidRequestException("Data file is empty"));

        mockMvc.perform(post("/api/v1/ingestions").contentType(MediaType.APPLICATION_JSON).content(VALID_BODY))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Data file is empty"));
    }

    @Test
    void unknownExecutionReturns404() throws Exception {
        UUID id = UUID.randomUUID();
        when(ingestionService.getStatus(id)).thenThrow(new ResourceNotFoundException("not found"));

        mockMvc.perform(get("/api/v1/ingestions/" + id)).andExpect(status().isNotFound());
    }

    @Test
    void knownExecutionReturnsItsStatus() throws Exception {
        UUID id = UUID.randomUUID();
        when(ingestionService.getStatus(id)).thenReturn(new IngestionStatusResponse(
                id, "in.csv", "SUCCESS", 20L, 20L, 0, null, null, null));

        mockMvc.perform(get("/api/v1/ingestions/" + id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.actualLoadedRecordCount").value(20));
    }

    @Test
    void airflowDownReturns502() throws Exception {
        when(ingestionService.startIngestion(any()))
                .thenThrow(new AirflowTriggerException("Unable to trigger Airflow DAG", null));

        mockMvc.perform(post("/api/v1/ingestions").contentType(MediaType.APPLICATION_JSON).content(VALID_BODY))
                .andExpect(status().isBadGateway());
    }

    @Test
    void brokenJsonReturns400() throws Exception {
        mockMvc.perform(post("/api/v1/ingestions").contentType(MediaType.APPLICATION_JSON).content("{not json"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void unexpectedErrorReturns500WithoutDetails() throws Exception {
        when(ingestionService.startIngestion(any())).thenThrow(new IllegalStateException("secret internal detail"));

        mockMvc.perform(post("/api/v1/ingestions").contentType(MediaType.APPLICATION_JSON).content(VALID_BODY))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.message").value("An unexpected error occurred"));
    }

    @Test
    void invalidExecutionIdReturns400() throws Exception {
        mockMvc.perform(get("/api/v1/ingestions/not-a-uuid")).andExpect(status().isBadRequest());
    }
}
