package com.amex.lumi.ingestion.service.impl;

import com.amex.lumi.ingestion.config.IngestionProperties;
import com.amex.lumi.ingestion.enums.FileType;
import com.amex.lumi.ingestion.exception.InvalidFileException;
import com.amex.lumi.ingestion.service.FileValidator;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

@Service
public class LocalFileValidator implements FileValidator {

    private final IngestionProperties ingestionProperties;

    public LocalFileValidator(IngestionProperties ingestionProperties) {
        this.ingestionProperties = ingestionProperties;
    }

    @Override
    public void validate(Path filePath, FileType fileType) {

        validateFileExists(filePath);

        validateFileReadable(filePath);

        validateFileNotEmpty(filePath);

        validateFileTypeSupported(fileType);

        validateFileExtension(filePath, fileType);
    }

    private void validateFileExists(Path filePath) {

        if (!Files.exists(filePath)) {
            throw new InvalidFileException(
                    "File does not exist: " + filePath
            );
        }

        if (!Files.isRegularFile(filePath)) {
            throw new InvalidFileException(
                    "Path is not a regular file: " + filePath
            );
        }
    }

    private void validateFileReadable(Path filePath) {

        if (!Files.isReadable(filePath)) {
            throw new InvalidFileException(
                    "File is not readable: " + filePath
            );
        }
    }

    private void validateFileNotEmpty(Path filePath) {

        try {
            if (Files.size(filePath) == 0) {
                throw new InvalidFileException(
                        "File is empty: " + filePath
                );
            }
        } catch (IOException exception) {
            throw new InvalidFileException(
                    "Unable to determine file size: " + filePath
            );
        }
    }

    private void validateFileTypeSupported(FileType fileType) {

        if (!ingestionProperties
                .getAllowedFileTypes()
                .contains(fileType)) {

            throw new InvalidFileException(
                    "Unsupported file type: " + fileType
            );
        }
    }

    private void validateFileExtension(
            Path filePath,
            FileType fileType) {

        String fileName = filePath
                .getFileName()
                .toString()
                .toLowerCase(Locale.ROOT);

        boolean valid = switch (fileType) {

            case JSON -> fileName.endsWith(".json");

            case CSV -> fileName.endsWith(".csv");

            case XML -> fileName.endsWith(".xml");

            case FIXED_WIDTH ->
                    fileName.endsWith(".txt");
        };

        if (!valid) {
            throw new InvalidFileException(
                    "File extension does not match file type "
                            + fileType
                            + ": "
                            + filePath
            );
        }
    }
}
