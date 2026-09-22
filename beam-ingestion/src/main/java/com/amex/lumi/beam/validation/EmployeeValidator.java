package com.amex.lumi.beam.validation;

import com.amex.lumi.beam.model.EmployeeRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Applies the warehouse field constraints to an employee record.
 *
 * <p>All applicable validation failures are collected so one invalid record
 * produces a complete diagnostic result.</p>
 */
public class EmployeeValidator {

    private static final int EMPLOYEE_ID_MAX_LENGTH = 7;
    private static final int FIRST_NAME_MAX_LENGTH = 15;
    private static final int LAST_NAME_MAX_LENGTH = 15;
    private static final int EMAIL_MAX_LENGTH = 30;
    private static final int DEPARTMENT_MAX_LENGTH = 20;
    private static final int JOB_TITLE_MAX_LENGTH = 30;
    private static final int EMPLOYMENT_STATUS_MAX_LENGTH = 13;

    private static final Pattern EMAIL_PATTERN =
            Pattern.compile(
                    "^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$"
            );

        /**
         * Validates all supported employee fields.
         *
         * @param employee employee record to validate
         * @return valid result or all field-level validation errors
         */
        public ValidationResult validate(
            EmployeeRecord employee) {

        List<String> errors =
                new ArrayList<>();

        validateEmployeeId(employee, errors);

        validateFirstName(employee, errors);

        validateLastName(employee, errors);

        validateEmail(employee, errors);

        validateDepartment(employee, errors);

        validateJobTitle(employee, errors);

        validateEmploymentStatus(employee, errors);

        validateManagerId(employee, errors);

        validateHireDate(employee, errors);

        return errors.isEmpty()
                ? ValidationResult.valid()
                : ValidationResult.invalid(errors);
    }

    private void validateEmployeeId(
            EmployeeRecord employee,
            List<String> errors) {

        String value =
                employee.getEmployeeId();

        if (value == null || value.isBlank()) {

            errors.add(
                    "employee_id is required"
            );

        } else if (value.length()
                > EMPLOYEE_ID_MAX_LENGTH) {

            errors.add(
                    "employee_id exceeds maximum length of "
                            + EMPLOYEE_ID_MAX_LENGTH
            );
        }
    }

    private void validateFirstName(
            EmployeeRecord employee,
            List<String> errors) {

        String value =
                employee.getFirstName();

        if (value == null || value.isBlank()) {

            errors.add(
                    "first_name is required"
            );

        } else if (value.length()
                > FIRST_NAME_MAX_LENGTH) {

            errors.add(
                    "first_name exceeds maximum length of "
                            + FIRST_NAME_MAX_LENGTH
            );
        }
    }

    private void validateLastName(
            EmployeeRecord employee,
            List<String> errors) {

        String value =
                employee.getLastName();

        if (value != null
                && value.length()
                > LAST_NAME_MAX_LENGTH) {

            errors.add(
                    "last_name exceeds maximum length of "
                            + LAST_NAME_MAX_LENGTH
            );
        }
    }

    private void validateEmail(
            EmployeeRecord employee,
            List<String> errors) {

        String value =
                employee.getEmail();

        if (value == null || value.isBlank()) {

            errors.add(
                    "email is required"
            );

            return;
        }

        if (value.length() > EMAIL_MAX_LENGTH) {

            errors.add(
                    "email exceeds maximum length of "
                            + EMAIL_MAX_LENGTH
            );
        }

        if (!EMAIL_PATTERN.matcher(value).matches()) {

            errors.add(
                    "email has invalid format"
            );
        }
    }

    private void validateDepartment(
            EmployeeRecord employee,
            List<String> errors) {

        String value =
                employee.getDepartment();

        if (value != null
                && value.length()
                > DEPARTMENT_MAX_LENGTH) {

            errors.add(
                    "department exceeds maximum length of "
                            + DEPARTMENT_MAX_LENGTH
            );
        }
    }

    private void validateJobTitle(
            EmployeeRecord employee,
            List<String> errors) {

        String value =
                employee.getJobTitle();

        if (value != null
                && value.length()
                > JOB_TITLE_MAX_LENGTH) {

            errors.add(
                    "job_title exceeds maximum length of "
                            + JOB_TITLE_MAX_LENGTH
            );
        }
    }

    private void validateEmploymentStatus(
            EmployeeRecord employee,
            List<String> errors) {

        String value =
                employee.getEmploymentStatus();

        if (value != null
                && value.length()
                > EMPLOYMENT_STATUS_MAX_LENGTH) {

            errors.add(
                    "employment_status exceeds maximum length of "
                            + EMPLOYMENT_STATUS_MAX_LENGTH
            );
        }
    }

    private void validateManagerId(
            EmployeeRecord employee,
            List<String> errors) {

        String value =
                employee.getManagerId();

        if (value == null || value.isBlank()) {

            errors.add(
                    "manager_id is required"
            );
        }
    }

    private void validateHireDate(
            EmployeeRecord employee,
            List<String> errors) {

        String value =
                employee.getHireDate();

        if (value == null || value.isBlank()) {

            errors.add(
                    "hire_date is required"
            );

            return;
        }

        try {

            java.time.LocalDate.parse(value);

        } catch (java.time.format.DateTimeParseException exception) {

            errors.add(
                    "hire_date has invalid format; expected yyyy-MM-dd"
            );
        }
    }
}
