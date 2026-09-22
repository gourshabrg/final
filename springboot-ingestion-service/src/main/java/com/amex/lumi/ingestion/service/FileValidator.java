package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.enums.FileType;

import java.nio.file.Path;

public interface FileValidator {

    void validate(Path filePath, FileType fileType);
}
