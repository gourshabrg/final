package com.amex.lumi.beam.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable employee fields; create with {@link #builder()}, copy with {@link #toBuilder()}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = EmployeeRecord.Builder.class)
public final class EmployeeRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("employee_id")
    private final String employeeId;
    @JsonProperty("first_name")
    private final String firstName;
    @JsonProperty("last_name")
    private final String lastName;
    private final String email;
    @JsonProperty("phone_number")
    private final String phoneNumber;
    @JsonProperty("hire_date")
    private final String hireDate;
    private final String department;
    @JsonProperty("job_title")
    private final String jobTitle;
    private final Long salary;
    private final String currency;
    @JsonProperty("employment_status")
    private final String employmentStatus;
    @JsonProperty("manager_id")
    private final String managerId;
    @JsonProperty("is_active")
    private final Boolean isActive;
    private final List<String> skills;
    private final Address address;
    @JsonProperty("emergency_contact")
    private final EmergencyContact emergencyContact;

    private EmployeeRecord(Builder builder) {
        this.employeeId = builder.employeeId;
        this.firstName = builder.firstName;
        this.lastName = builder.lastName;
        this.email = builder.email;
        this.phoneNumber = builder.phoneNumber;
        this.hireDate = builder.hireDate;
        this.department = builder.department;
        this.jobTitle = builder.jobTitle;
        this.salary = builder.salary;
        this.currency = builder.currency;
        this.employmentStatus = builder.employmentStatus;
        this.managerId = builder.managerId;
        this.isActive = builder.isActive;
        // Own read-only copy (ArrayList keeps null entries, List.copyOf would not).
        this.skills = builder.skills == null ? null : Collections.unmodifiableList(new ArrayList<>(builder.skills));
        this.address = builder.address;
        this.emergencyContact = builder.emergencyContact;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder().employeeId(employeeId).firstName(firstName).lastName(lastName).email(email)
                .phoneNumber(phoneNumber).hireDate(hireDate).department(department).jobTitle(jobTitle)
                .salary(salary).currency(currency).employmentStatus(employmentStatus).managerId(managerId)
                .isActive(isActive).skills(skills).address(address).emergencyContact(emergencyContact);
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

    public Long getSalary() {
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

    public Boolean getIsActive() {
        return isActive;
    }

    public List<String> getSkills() {
        return skills;
    }

    public Address getAddress() {
        return address;
    }

    public EmergencyContact getEmergencyContact() {
        return emergencyContact;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof EmployeeRecord that)) {
            return false;
        }
        return Objects.equals(employeeId, that.employeeId)
                && Objects.equals(firstName, that.firstName)
                && Objects.equals(lastName, that.lastName)
                && Objects.equals(email, that.email)
                && Objects.equals(phoneNumber, that.phoneNumber)
                && Objects.equals(hireDate, that.hireDate)
                && Objects.equals(department, that.department)
                && Objects.equals(jobTitle, that.jobTitle)
                && Objects.equals(salary, that.salary)
                && Objects.equals(currency, that.currency)
                && Objects.equals(employmentStatus, that.employmentStatus)
                && Objects.equals(managerId, that.managerId)
                && Objects.equals(isActive, that.isActive)
                && Objects.equals(skills, that.skills)
                && Objects.equals(address, that.address)
                && Objects.equals(emergencyContact, that.emergencyContact);
    }

    @Override
    public int hashCode() {
        return Objects.hash(employeeId, firstName, lastName, email, phoneNumber, hireDate,
                department, jobTitle, salary, currency, employmentStatus, managerId,
                isActive, skills, address, emergencyContact);
    }

    // Only the id, so salary and phone never reach the logs.
    @Override
    public String toString() {
        return "EmployeeRecord{employeeId='" + employeeId + "'}";
    }

    /** Also used by Jackson to read JSON; the @JsonProperty names are the field names in the files. */
    @JsonPOJOBuilder(withPrefix = "")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Builder {
        private String employeeId;
        private String firstName;
        private String lastName;
        private String email;
        private String phoneNumber;
        private String hireDate;
        private String department;
        private String jobTitle;
        private Long salary;
        private String currency;
        private String employmentStatus;
        private String managerId;
        private Boolean isActive;
        private List<String> skills;
        private Address address;
        private EmergencyContact emergencyContact;

        @JsonProperty("employee_id")
        public Builder employeeId(String value) {
            this.employeeId = value;
            return this;
        }

        @JsonProperty("first_name")
        public Builder firstName(String value) {
            this.firstName = value;
            return this;
        }

        @JsonProperty("last_name")
        public Builder lastName(String value) {
            this.lastName = value;
            return this;
        }

        public Builder email(String value) {
            this.email = value;
            return this;
        }

        @JsonProperty("phone_number")
        public Builder phoneNumber(String value) {
            this.phoneNumber = value;
            return this;
        }

        @JsonProperty("hire_date")
        public Builder hireDate(String value) {
            this.hireDate = value;
            return this;
        }

        public Builder department(String value) {
            this.department = value;
            return this;
        }

        @JsonProperty("job_title")
        public Builder jobTitle(String value) {
            this.jobTitle = value;
            return this;
        }

        public Builder salary(Long value) {
            this.salary = value;
            return this;
        }

        public Builder currency(String value) {
            this.currency = value;
            return this;
        }

        @JsonProperty("employment_status")
        public Builder employmentStatus(String value) {
            this.employmentStatus = value;
            return this;
        }

        @JsonProperty("manager_id")
        public Builder managerId(String value) {
            this.managerId = value;
            return this;
        }

        @JsonProperty("is_active")
        public Builder isActive(Boolean value) {
            this.isActive = value;
            return this;
        }

        public Builder skills(List<String> value) {
            this.skills = value;
            return this;
        }

        public Builder address(Address value) {
            this.address = value;
            return this;
        }

        @JsonProperty("emergency_contact")
        public Builder emergencyContact(EmergencyContact value) {
            this.emergencyContact = value;
            return this;
        }

        public EmployeeRecord build() {
            return new EmployeeRecord(this);
        }
    }
}
