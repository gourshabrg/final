package com.amex.lumi.beam.read;

import com.amex.lumi.beam.TestEmployees;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CsvEmployeeParserTest {

    private final CsvEmployeeParser parser = new CsvEmployeeParser();

    private RecordCollector parse(String csv) throws IOException {
        RecordCollector collector = new RecordCollector();
        parser.parse(new StringReader(csv), collector);
        return collector;
    }

    @Test
    void parsesAllColumnsIncludingQuotedComma() throws IOException {
        RecordCollector result = parse(TestEmployees.CSV_HEADER + "\n" + TestEmployees.CSV_ROW + "\n");

        assertEquals(List.of(TestEmployees.valid()), result.records);
        assertEquals(List.of(), result.errors);
    }

    @Test
    void emptyCellsBecomeNull() throws IOException {
        String row = TestEmployees.CSV_ROW.replace(",Sharma,", ",,");
        RecordCollector result = parse(TestEmployees.CSV_HEADER + "\n" + row);

        assertNull(result.records.get(0).getLastName());
    }

    @Test
    void badSalaryIsReportedWithoutTheValue() throws IOException {
        String row = TestEmployees.CSV_ROW.replace(",950000,", ",abc,");
        RecordCollector result = parse(TestEmployees.CSV_HEADER + "\n" + row);

        assertEquals(List.of("salary must be a whole number"), result.errors);
    }

    @Test
    void isActiveMustBeTrueOrFalse() throws IOException {
        String row = TestEmployees.CSV_ROW.replace(",true,", ",yes,");
        RecordCollector result = parse(TestEmployees.CSV_HEADER + "\n" + row);

        assertEquals(List.of("is_active must be true or false"), result.errors);
    }

    @Test
    void rowWithWrongColumnCountIsReported() throws IOException {
        RecordCollector result = parse(TestEmployees.CSV_HEADER + "\nEMP0002,Only,Three\n" + TestEmployees.CSV_ROW);

        assertEquals(1, result.records.size());
        assertEquals(List.of("row has 3 column(s) but the header has 23"), result.errors);
    }

    @Test
    void headerOnlyFileHasNoRecords() throws IOException {
        RecordCollector result = parse(TestEmployees.CSV_HEADER + "\n");

        assertEquals(List.of(), result.records);
        assertEquals(List.of(), result.errors);
    }

    @Test
    void skillsAreSplitAndTrimmed() throws IOException {
        String row = TestEmployees.CSV_ROW.replace("Python;Docker", " Java ; ;SQL ");
        RecordCollector result = parse(TestEmployees.CSV_HEADER + "\n" + row);

        assertEquals(List.of("Java", "SQL"), result.records.get(0).getSkills());
    }

    @Test
    void missingOptionalColumnBecomesNull() throws IOException {
        String header = TestEmployees.CSV_HEADER.replace(",department", "");
        String row = TestEmployees.CSV_ROW.replace(",Engineering", "");
        RecordCollector result = parse(header + "\n" + row);

        assertNull(result.records.get(0).getDepartment());
    }
}
