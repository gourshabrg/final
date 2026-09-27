package com.amex.lumi.beam.read;

import com.amex.lumi.beam.TestEmployees;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
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

    @Test
    void splitFileRecordKeepsItsNumberFromTheOriginalFile() throws IOException {
        RecordCollector result = parse(TestEmployees.CSV_HEADER + ",source_record_number\n"
                + TestEmployees.CSV_ROW + ",87\n");

        assertEquals(List.of(87L), result.recordNumbers);
        assertEquals("EMP0001", result.records.get(0).getEmployeeId());
    }

    @Test
    void fileWithoutTheColumnHasNoOriginalNumber() throws IOException {
        RecordCollector result = parse(TestEmployees.CSV_HEADER + "\n" + TestEmployees.CSV_ROW + "\n");

        assertEquals(Collections.singletonList(null), result.recordNumbers);
    }

    @Test
    void badRowInSplitFileKeepsItsNumber() throws IOException {
        String badSalary = TestEmployees.CSV_ROW.replace(",950000,", ",lots,");
        RecordCollector result = parse(TestEmployees.CSV_HEADER + ",source_record_number\n" + badSalary + ",12\n"
                + "EMP0002,too,few\n");

        assertEquals(List.of(12L), result.errorNumbers.subList(0, 1));
        // Too short to reach the number column: no original number.
        assertNull(result.errorNumbers.get(1));
    }

    @Test
    void missingRequiredColumnIsOneFileErrorNotAnErrorPerRow() throws IOException {
        String header = TestEmployees.CSV_HEADER.replace(",email,", ",emial,");
        RecordCollector result = parse(header + "\n" + TestEmployees.CSV_ROW + "\n" + TestEmployees.CSV_ROW + "\n");

        assertEquals(List.of("CSV header is missing required column(s): email"), result.fileErrors);
        assertEquals(List.of(), result.records);
        assertEquals(List.of(), result.errors);
    }

    @Test
    void unknownColumnIsIgnored() throws IOException {
        RecordCollector result = parse(TestEmployees.CSV_HEADER + ",notes\n" + TestEmployees.CSV_ROW + ",hello\n");

        assertEquals(List.of(TestEmployees.valid()), result.records);
    }

    @Test
    void rowMarkedCorruptBySparkIsAParseError() throws IOException {
        String header = TestEmployees.CSV_HEADER + ",_corrupt_record,source_record_number\n";
        RecordCollector result = parse(header
                + TestEmployees.CSV_ROW + ",,1\n"
                + TestEmployees.CSV_ROW.replace("EMP0001", "EMP0002") + ",\"EMP0002,raw,row\",2\n");

        assertEquals(1, result.records.size());
        assertEquals(List.of(CsvEmployeeParser.SPARK_CORRUPT_ROW), result.errors);
        assertEquals(List.of(2L), result.errorNumbers);
    }
}
