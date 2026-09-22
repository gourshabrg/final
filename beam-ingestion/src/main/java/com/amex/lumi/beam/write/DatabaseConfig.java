package com.amex.lumi.beam.write;

import com.amex.lumi.beam.common.PipelineConstants;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Warehouse connection settings. Serializable so Beam can send it to every worker.
 */
public record DatabaseConfig(String jdbcUrl, String username, String password) implements Serializable {

    /** Opens a connection. We commit ourselves, so auto-commit is off. */
    public Connection openConnection() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        properties.setProperty("options", "-c TimeZone=" + PipelineConstants.TIME_ZONE);
        Connection connection = DriverManager.getConnection(jdbcUrl, properties);
        connection.setAutoCommit(false);
        return connection;
    }

    // Never print the real password in logs.
    @Override
    public String toString() {
        return "DatabaseConfig{jdbcUrl='" + jdbcUrl + "', username='" + username + "', password='****'}";
    }
}
