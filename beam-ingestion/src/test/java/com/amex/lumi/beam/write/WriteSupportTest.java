package com.amex.lumi.beam.write;

import com.amex.lumi.beam.model.RecordFailure;
import com.amex.lumi.beam.model.RecordFailure.FailureType;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteSupportTest {

    @Test
    void valueTooLongIsABadRow() {
        assertTrue(JdbcSupport.isRecordLevelError(new SQLException("too long", "22001")));
    }

    @Test
    void duplicateKeyIsABadRow() {
        assertTrue(JdbcSupport.isRecordLevelError(new SQLException("duplicate", "23505")));
    }

    @Test
    void connectionErrorIsNotABadRow() {
        assertFalse(JdbcSupport.isRecordLevelError(new SQLException("connection refused", "08001")));
        assertFalse(JdbcSupport.isRecordLevelError(new SQLException("no state")));
    }

    @Test
    void databaseConfigHidesPassword() {
        String text = new DatabaseConfig("jdbc:postgresql://db/warehouse", "airflow", "secret").toString();

        assertTrue(text.contains("****"));
        assertFalse(text.contains("secret"));
    }

    @Test
    void errorLineHandlesParseErrorWithoutEmployee() {
        RecordFailure failure = new RecordFailure(2, "in.csv", "exec-1", FailureType.PARSE_ERROR,
                "salary must be a whole number", null);

        assertNull(failure.employeeId());
        assertEquals("record_number=2|error_type=PARSE_ERROR|error_message=salary must be a whole number"
                + "|employee_id=|source_file=in.csv|execution_id=exec-1", ErrorLineFormatter.format(failure));
    }
}
