package com.amex.lumi.beam.validate;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fails if the table column sizes and the validator's max lengths drift apart.
 */
class ColumnLengthContractTest {

    private static final Path TABLE_SCRIPT = Path.of("..", "database", "init", "02-create-warehouse-tables.sql");
    private static final Pattern TEXT_COLUMN = Pattern.compile("(\\w+)\\s+(?:VARCHAR|CHAR)\\((\\d+)\\)");

    @Test
    void validatorMaxLengthsMatchTheEmployeeTable() throws IOException {
        Map<String, Integer> columns = employeeColumnSizes();
        int compared = 0;
        for (Map.Entry<String, Integer> rule : EmployeeValidator.maxLengths().entrySet()) {
            Integer columnSize = columns.get(rule.getKey());
            if (columnSize != null) {
                assertEquals(columnSize, rule.getValue(), "max length of " + rule.getKey());
                compared++;
            }
        }
        // Guards against the regex silently matching nothing.
        assertTrue(compared >= 9, "only " + compared + " columns compared");
    }

    private static Map<String, Integer> employeeColumnSizes() throws IOException {
        String script = Files.readString(TABLE_SCRIPT);
        String employeeTable = script.substring(script.indexOf("CREATE TABLE IF NOT EXISTS employee"),
                script.indexOf("CREATE TABLE IF NOT EXISTS ingestion_error"));
        Map<String, Integer> sizes = new HashMap<>();
        Matcher matcher = TEXT_COLUMN.matcher(employeeTable);
        while (matcher.find()) {
            sizes.put(matcher.group(1), Integer.parseInt(matcher.group(2)));
        }
        return sizes;
    }
}
