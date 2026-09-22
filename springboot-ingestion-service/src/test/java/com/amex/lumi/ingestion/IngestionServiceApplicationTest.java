package com.amex.lumi.ingestion;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/** Checks that all beans and settings wire together. No database connection is opened. */
@SpringBootTest(properties = "lumi.encryption.key=0123456789abcdef0123456789abcdef")
class IngestionServiceApplicationTest {

    @Test
    void contextLoads() {
    }
}
