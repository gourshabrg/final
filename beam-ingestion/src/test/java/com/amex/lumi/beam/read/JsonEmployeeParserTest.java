package com.amex.lumi.beam.read;

import com.amex.lumi.beam.TestEmployees;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonEmployeeParserTest {

    private static final String EMPLOYEE_JSON = """
            {"employee_id":"EMP0001","first_name":"Arjun","last_name":"Sharma",
             "email":"arjun.sharma@techcorp.com","phone_number":"9876543210","hire_date":"2022-03-15",
             "department":"Engineering","job_title":"Senior Backend Engineer","salary":950000,
             "currency":"INR","employment_status":"Full-time","manager_id":"MGR0001","is_active":true,
             "skills":["Python","Docker"],
             "address":{"street":"102, Silicon Heights","city":"Bengaluru","state":"Karnataka",
                        "postal_code":"560100","country":"India"},
             "emergency_contact":{"name":"Priya Sharma","relationship":"Spouse",
                                  "phone":"9876543211","email":"priya.s@example.com"}}
            """.replace("\n", "");

    private final JsonEmployeeParser parser = new JsonEmployeeParser();

    private RecordCollector parse(String json) throws IOException {
        RecordCollector collector = new RecordCollector();
        parser.parse(new StringReader(json), collector);
        return collector;
    }

    @Test
    void parsesJsonArray() throws IOException {
        RecordCollector result = parse("[" + EMPLOYEE_JSON + "," + EMPLOYEE_JSON + "]");

        assertEquals(List.of(TestEmployees.valid(), TestEmployees.valid()), result.records);
    }

    @Test
    void parsesJsonLinesFromPySpark() throws IOException {
        RecordCollector result = parse(EMPLOYEE_JSON + "\n" + EMPLOYEE_JSON + "\n");

        assertEquals(2, result.records.size());
    }

    @Test
    void wrongTypeRejectsOnlyThatRecord() throws IOException {
        String badSalary = EMPLOYEE_JSON.replace("\"salary\":950000", "\"salary\":\"lots\"");
        RecordCollector result = parse("[" + badSalary + "," + EMPLOYEE_JSON + "]");

        assertEquals(1, result.records.size());
        assertEquals(List.of("invalid value for field 'salary'"), result.errors);
    }

    @Test
    void brokenJsonFailsTheFile() {
        assertThrows(IOException.class, () -> parse("[" + EMPLOYEE_JSON + ", {oops"));
    }

    @Test
    void emptyFileHasNoRecords() throws IOException {
        RecordCollector result = parse("   ");

        assertEquals(List.of(), result.records);
        assertEquals(List.of(), result.errors);
    }

    @Test
    void unknownFieldsAreIgnored() throws IOException {
        RecordCollector result = parse("[" + EMPLOYEE_JSON.replace("{\"employee_id\"", "{\"extra\":1,\"employee_id\"") + "]");

        assertEquals(List.of(TestEmployees.valid()), result.records);
    }

    @Test
    void plainValueInsteadOfObjectFailsTheFile() {
        assertThrows(IOException.class, () -> parse("\"just text\""));
    }
}
