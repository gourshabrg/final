package com.amex.lumi.ingestion;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class DatabaseConnectivityIntegrationTest {

    static {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void shouldConnectToWarehouseDatabaseAndReadData() {
        String databaseName = jdbcTemplate.queryForObject("SELECT current_database()", String.class);
        Integer employeeCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM employee", Integer.class);

        assertThat(databaseName).isEqualTo("warehouse");
        assertThat(employeeCount).isNotNull().isPositive();
    }
}
