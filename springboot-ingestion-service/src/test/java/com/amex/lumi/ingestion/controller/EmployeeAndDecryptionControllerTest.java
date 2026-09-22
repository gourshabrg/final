package com.amex.lumi.ingestion.controller;

import com.amex.lumi.ingestion.dto.EmployeeResponse;
import com.amex.lumi.ingestion.exception.DecryptionException;
import com.amex.lumi.ingestion.exception.ResourceNotFoundException;
import com.amex.lumi.ingestion.security.FieldDecryptor;
import com.amex.lumi.ingestion.service.EmployeeService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest({EmployeeController.class, DecryptionController.class})
class EmployeeAndDecryptionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private EmployeeService employeeService;

    @MockitoBean
    private FieldDecryptor decryptor;

    @Test
    void returnsDecryptedEmployee() throws Exception {
        when(employeeService.getDecryptedEmployee("EMP0001")).thenReturn(new EmployeeResponse(
                "EMP0001", "Arjun", "Sharma", "arjun@techcorp.com", "9876543210", null, null, null,
                950000L, "INR", "Full-time", "MGR0001", true, List.of(), null, null, null, null, null));

        mockMvc.perform(get("/api/v1/employees/EMP0001"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.phone_number").value("9876543210"))
                .andExpect(jsonPath("$.salary").value(950000));
    }

    @Test
    void unknownEmployeeReturns404() throws Exception {
        when(employeeService.getDecryptedEmployee("NOPE000"))
                .thenThrow(new ResourceNotFoundException("Employee not found: NOPE000"));

        mockMvc.perform(get("/api/v1/employees/NOPE000"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.message").value("Employee not found: NOPE000"));
    }

    @Test
    void decryptsOneValue() throws Exception {
        when(decryptor.decrypt("v1:abc:def")).thenReturn("950000");

        mockMvc.perform(post("/api/v1/decrypt").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"encryptedValue\":\"v1:abc:def\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.decryptedValue").value("950000"));
    }

    @Test
    void blankValueReturns400() throws Exception {
        mockMvc.perform(post("/api/v1/decrypt").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"encryptedValue\":\"\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("encryptedValue is required"));
    }

    @Test
    void valueThatCannotBeDecryptedReturns400() throws Exception {
        when(decryptor.decrypt("garbage")).thenThrow(new DecryptionException("Value could not be decrypted", null));

        mockMvc.perform(post("/api/v1/decrypt").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"encryptedValue\":\"garbage\"}"))
                .andExpect(status().isBadRequest());
    }
}
