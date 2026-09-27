package com.amex.lumi.beam.write;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Small database helpers shared by all writers.
 */
final class JdbcSupport {

    private JdbcSupport() {
    }

    /**
     * SQLSTATE 22xxx/23xxx means a bad row; anything else is a database problem.
     */
    static boolean isRecordLevelError(SQLException exception) {
        String state = exception.getSQLState();
        return state != null && (state.startsWith("22") || state.startsWith("23"));
    }

    static void rollbackQuietly(Connection connection, Logger logger) {
        if (connection == null) {
            return;
        }
        try {
            connection.rollback();
        } catch (SQLException exception) {
            logger.error("Rollback failed", exception);
        }
    }

    static void closeQuietly(AutoCloseable resource, Logger logger) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception exception) {
            logger.warn("Could not close {}", resource.getClass().getSimpleName(), exception);
        }
    }

    /** Saves an object as a JSON column (JSONB), or NULL when the object is null. */
    static void setJsonb(PreparedStatement statement, int index, Object value, ObjectMapper mapper)
            throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.OTHER);
            return;
        }
        PGobject json = new PGobject();
        json.setType("jsonb");
        try {
            json.setValue(mapper.writeValueAsString(value));
        } catch (JsonProcessingException exception) {
            throw new SQLException("Could not convert value to JSON", exception);
        }
        statement.setObject(index, json);
    }
}
