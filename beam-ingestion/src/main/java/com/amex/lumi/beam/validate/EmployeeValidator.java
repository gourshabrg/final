package com.amex.lumi.beam.validate;

import com.amex.lumi.beam.model.EmployeeRecord;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Field rules from the requirement. Returns all errors of a record, not just the first.
 */
public class EmployeeValidator {

    private static final Pattern EMAIL = Pattern.compile("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$");
    private static final Pattern CURRENCY = Pattern.compile("^[A-Z]{3}$");
    private static final int SKILLS_MAX_LENGTH = 100;

    private static final List<LengthRule> LENGTH_RULES = List.of(
            LengthRule.required("employee_id", EmployeeRecord::getEmployeeId, 7, 7),
            LengthRule.required("first_name", EmployeeRecord::getFirstName, 3, 15),
            LengthRule.optional("last_name", EmployeeRecord::getLastName, 15),
            LengthRule.required("email", EmployeeRecord::getEmail, 13, 30),
            LengthRule.required("phone_number", EmployeeRecord::getPhoneNumber, 10, 10),
            LengthRule.required("hire_date", EmployeeRecord::getHireDate, 10, 10),
            LengthRule.optional("department", EmployeeRecord::getDepartment, 20),
            LengthRule.optional("job_title", EmployeeRecord::getJobTitle, 30),
            LengthRule.required("currency", EmployeeRecord::getCurrency, 3, 3),
            LengthRule.required("employment_status", EmployeeRecord::getEmploymentStatus, 3, 13),
            LengthRule.required("manager_id", EmployeeRecord::getManagerId, 7, 7));

    /** Empty list = valid. */
    public List<String> validate(EmployeeRecord employee) {
        List<String> errors = new ArrayList<>();
        for (LengthRule rule : LENGTH_RULES) {
            rule.check(employee, errors);
        }
        checkFormats(employee, errors);
        return errors;
    }

    // Only checked when present; "required" is already reported above.
    private static void checkFormats(EmployeeRecord employee, List<String> errors) {
        String email = employee.getEmail();
        if (!isBlank(email) && !EMAIL.matcher(email).matches()) {
            errors.add("email has an invalid format");
        }

        String hireDate = employee.getHireDate();
        if (!isBlank(hireDate)) {
            try {
                LocalDate.parse(hireDate);
            } catch (DateTimeParseException exception) {
                errors.add("hire_date must be a real date in yyyy-MM-dd format");
            }
        }

        String currency = employee.getCurrency();
        if (!isBlank(currency) && !CURRENCY.matcher(currency).matches()) {
            errors.add("currency must be a 3-letter uppercase ISO code such as INR or USD");
        }

        if (employee.getSalary() != null && employee.getSalary() < 0) {
            errors.add("salary must not be negative");
        }

        if (employee.getIsActive() == null) {
            errors.add("is_active is required (true or false)");
        }

        List<String> skills = employee.getSkills();
        if (skills != null && String.join(";", skills).length() > SKILLS_MAX_LENGTH) {
            errors.add("skills must not exceed " + SKILLS_MAX_LENGTH + " characters in total");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    /** Field length must be between min and max. */
    private record LengthRule(String field, Function<EmployeeRecord, String> getter,
                              int min, int max, boolean required) {

        static LengthRule required(String field, Function<EmployeeRecord, String> getter, int min, int max) {
            return new LengthRule(field, getter, min, max, true);
        }

        static LengthRule optional(String field, Function<EmployeeRecord, String> getter, int max) {
            return new LengthRule(field, getter, 0, max, false);
        }

        void check(EmployeeRecord employee, List<String> errors) {
            String value = getter.apply(employee);
            if (isBlank(value)) {
                if (required) {
                    errors.add(field + " is required");
                }
                return;
            }
            int length = value.length();
            if (length < min || length > max) {
                errors.add(min == max
                        ? field + " must be exactly " + max + " characters (found " + length + ")"
                        : field + " must be between " + min + " and " + max + " characters (found " + length + ")");
            }
        }
    }
}
