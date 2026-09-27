package com.amex.lumi.beam.validate;

import com.amex.lumi.beam.common.PipelineConstants;
import com.amex.lumi.beam.model.EmployeeRecord;

import java.time.Clock;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Currency;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Field rules from the requirement. Returns all errors of a record, not just the first.
 */
public class EmployeeValidator {

    private static final Pattern EMAIL = Pattern.compile("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$");
    private static final Pattern DIGITS = Pattern.compile("\\d+");
    private static final Pattern LETTERS_AND_DIGITS = Pattern.compile("[A-Za-z0-9]+");
    private static final int SKILLS_MAX_LENGTH = 100;
    private static final Set<String> ISO_CURRENCIES = Currency.getAvailableCurrencies().stream()
            .map(Currency::getCurrencyCode)
            .collect(Collectors.toUnmodifiableSet());
    // Sorted so the error message always lists them in the same order.
    private static final Set<String> EMPLOYMENT_STATUSES = Collections.unmodifiableSortedSet(new TreeSet<>(
            List.of("Contract", "Full-time", "Intern", "Part-time", "Temporary")));

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

    // "Today" for the future-date check; tests pass a fixed clock.
    private final Clock clock;

    public EmployeeValidator() {
        this(Clock.systemUTC());
    }

    EmployeeValidator(Clock clock) {
        this.clock = clock;
    }

    /** Maximum length of each checked field, e.g. first_name -> 15. A test compares it with the table columns. */
    static Map<String, Integer> maxLengths() {
        return LENGTH_RULES.stream().collect(Collectors.toMap(LengthRule::field, LengthRule::max));
    }

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
    private void checkFormats(EmployeeRecord employee, List<String> errors) {
        checkPattern(employee.getPhoneNumber(), DIGITS, "phone_number must contain digits only", errors);
        checkPattern(employee.getEmployeeId(), LETTERS_AND_DIGITS, "employee_id must contain only letters and digits",
                errors);
        checkPattern(employee.getManagerId(), LETTERS_AND_DIGITS, "manager_id must contain only letters and digits",
                errors);

        String email = employee.getEmail();
        if (!isBlank(email) && !EMAIL.matcher(email).matches()) {
            errors.add("email has an invalid format");
        }

        String hireDate = employee.getHireDate();
        if (!isBlank(hireDate)) {
            try {
                if (LocalDate.parse(hireDate).isAfter(LocalDate.now(clock))) {
                    errors.add("hire_date must not be in the future");
                }
            } catch (DateTimeParseException exception) {
                errors.add("hire_date must be a real date in yyyy-MM-dd format");
            }
        }

        String currency = employee.getCurrency();
        if (!isBlank(currency) && !ISO_CURRENCIES.contains(currency)) {
            errors.add("currency must be an ISO 4217 code such as INR or USD");
        }

        String status = employee.getEmploymentStatus();
        if (!isBlank(status) && !EMPLOYMENT_STATUSES.contains(status)) {
            errors.add("employment_status must be one of: " + String.join(", ", EMPLOYMENT_STATUSES));
        }

        if (employee.getSalary() != null && employee.getSalary() < 0) {
            errors.add("salary must not be negative");
        }

        if (employee.getIsActive() == null) {
            errors.add("is_active is required (true or false)");
        }

        List<String> skills = employee.getSkills();
        if (skills != null && String.join(PipelineConstants.SKILL_SEPARATOR, skills).length() > SKILLS_MAX_LENGTH) {
            errors.add("skills must not exceed " + SKILLS_MAX_LENGTH + " characters in total");
        }
    }

    private static void checkPattern(String value, Pattern pattern, String message, List<String> errors) {
        if (!isBlank(value) && !pattern.matcher(value).matches()) {
            errors.add(message);
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
