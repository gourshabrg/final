package com.amex.lumi.beam.config;

import java.io.Serializable;

/**
 * Serializable PostgreSQL connection settings passed to pipeline components.
 */
public class DatabaseConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public DatabaseConfig(
            String jdbcUrl,
            String username,
            String password) {

        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
