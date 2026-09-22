package com.amex.lumi.beam.sink;

import com.amex.lumi.beam.model.Address;
import com.amex.lumi.beam.model.EmployeeRecord;
import com.amex.lumi.beam.model.EmergencyContact;

import java.io.Serializable;
import java.util.List;

/**
 * Safe employee snapshot for error storage with sensitive values redacted.
 */
public class SanitizedErrorRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String employeeId;
    private final String firstName;
    private final String lastName;
    private final String email;

    /*
     * Sensitive field.
     * Never store the actual value in the error record.
     */
    private final String phoneNumber;

    private final String hireDate;
    private final String department;
    private final String jobTitle;

    /*
     * Sensitive field.
     */
    private final String salary;

    private final String currency;
    private final String employmentStatus;
    private final String managerId;
    private final Boolean active;
    private final List<String> skills;

    private final Address address;

    /*
     * Emergency contact remains structured.
     * Only its phone number is sanitized.
     */
    private final SanitizedEmergencyContact emergencyContact;

    private SanitizedErrorRecord(
            String employeeId,
            String firstName,
            String lastName,
            String email,
            String phoneNumber,
            String hireDate,
            String department,
            String jobTitle,
            String salary,
            String currency,
            String employmentStatus,
            String managerId,
            Boolean active,
            List<String> skills,
            Address address,
            SanitizedEmergencyContact emergencyContact) {

        this.employeeId = employeeId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.phoneNumber = phoneNumber;
        this.hireDate = hireDate;
        this.department = department;
        this.jobTitle = jobTitle;
        this.salary = salary;
        this.currency = currency;
        this.employmentStatus = employmentStatus;
        this.managerId = managerId;
        this.active = active;
        this.skills = skills;
        this.address = address;
        this.emergencyContact = emergencyContact;
    }

    public static SanitizedErrorRecord from(
            EmployeeRecord employee) {

        EmergencyContact emergencyContact =
                employee.getEmergencyContact();

        SanitizedEmergencyContact sanitizedEmergencyContact =
                emergencyContact == null
                        ? null
                        : SanitizedEmergencyContact.from(
                                emergencyContact
                        );

        return new SanitizedErrorRecord(
                employee.getEmployeeId(),
                employee.getFirstName(),
                employee.getLastName(),
                employee.getEmail(),

                /*
                 * Sensitive phone number.
                 */
                redact(employee.getPhoneNumber()),

                employee.getHireDate(),
                employee.getDepartment(),
                employee.getJobTitle(),

                /*
                 * Sensitive salary.
                 */
                employee.getSalary() == null
                        ? null
                        : "[REDACTED]",

                employee.getCurrency(),
                employee.getEmploymentStatus(),
                employee.getManagerId(),
                employee.getIsActive(),
                employee.getSkills(),
                employee.getAddress(),
                sanitizedEmergencyContact
        );
    }

    private static String redact(
            String value) {

        if (value == null) {
            return null;
        }

        return "[REDACTED]";
    }

    public String getEmployeeId() {
        return employeeId;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getEmail() {
        return email;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public String getHireDate() {
        return hireDate;
    }

    public String getDepartment() {
        return department;
    }

    public String getJobTitle() {
        return jobTitle;
    }

    public String getSalary() {
        return salary;
    }

    public String getCurrency() {
        return currency;
    }

    public String getEmploymentStatus() {
        return employmentStatus;
    }

    public String getManagerId() {
        return managerId;
    }

    public Boolean getActive() {
        return active;
    }

    public List<String> getSkills() {
        return skills;
    }

    public Address getAddress() {
        return address;
    }

    public SanitizedEmergencyContact getEmergencyContact() {
        return emergencyContact;
    }
}
