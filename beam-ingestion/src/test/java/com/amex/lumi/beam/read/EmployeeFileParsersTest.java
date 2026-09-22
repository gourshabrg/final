package com.amex.lumi.beam.read;

import com.amex.lumi.beam.model.FileType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class EmployeeFileParsersTest {

    @Test
    void csvGetsCsvParser() {
        assertInstanceOf(CsvEmployeeParser.class, EmployeeFileParsers.forType(FileType.CSV));
    }

    @Test
    void jsonGetsJsonParser() {
        assertInstanceOf(JsonEmployeeParser.class, EmployeeFileParsers.forType(FileType.JSON));
    }
}
