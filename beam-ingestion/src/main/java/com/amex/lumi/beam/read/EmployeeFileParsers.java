package com.amex.lumi.beam.read;

import com.amex.lumi.beam.model.FileType;

/**
 * Returns the parser for a file type.
 */
public final class EmployeeFileParsers {

    private EmployeeFileParsers() {
    }

    public static EmployeeFileParser forType(FileType fileType) {
        return switch (fileType) {
            case CSV -> new CsvEmployeeParser();
            case JSON -> new JsonEmployeeParser();
        };
    }
}
