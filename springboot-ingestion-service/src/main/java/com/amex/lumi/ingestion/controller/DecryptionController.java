package com.amex.lumi.ingestion.controller;

import com.amex.lumi.ingestion.dto.DecryptionRequest;
import com.amex.lumi.ingestion.dto.DecryptionResponse;
import com.amex.lumi.ingestion.security.FieldDecryptor;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/decrypt")
public class DecryptionController {

    private final FieldDecryptor decryptor;

    public DecryptionController(FieldDecryptor decryptor) {
        this.decryptor = decryptor;
    }

    /** Decrypts one value copied from the database, e.g. salary_encrypted. */
    @PostMapping
    public DecryptionResponse decrypt(@Valid @RequestBody DecryptionRequest request) {
        return new DecryptionResponse(decryptor.decrypt(request.encryptedValue()));
    }
}
