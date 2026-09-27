package com.amex.lumi.beam.write;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertFalse;
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
}
