package com.amex.lumi.beam.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Employee fields from any file format. Has setters because the parsers fill it field by field.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EmployeeRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("employee_id")
    private String employeeId;

    @JsonProperty("first_name")
    private String firstName;

    @JsonProperty("last_name")
    private String lastName;

    private String email;

    @JsonProperty("phone_number")
    private String phoneNumber;

    @JsonProperty("hire_date")
    private String hireDate;

    private String department;

    @JsonProperty("job_title")
    private String jobTitle;

    private Long salary;

    private String currency;

    @JsonProperty("employment_status")
    private String employmentStatus;

    @JsonProperty("manager_id")
    private String managerId;

    @JsonProperty("is_active")
    private Boolean isActive;

    private List<String> skills;

    private Address address;

    @JsonProperty("emergency_contact")
    private EmergencyContact emergencyContact;

    public String getEmployeeId() {
        return employeeId;
    }

    public void setEmployeeId(String employeeId) {
        this.employeeId = employeeId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getHireDate() {
        return hireDate;
    }

    public void setHireDate(String hireDate) {
        this.hireDate = hireDate;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public String getJobTitle() {
        return jobTitle;
    }

    public void setJobTitle(String jobTitle) {
        this.jobTitle = jobTitle;
    }

    public Long getSalary() {
        return salary;
    }

    public void setSalary(Long salary) {
        this.salary = salary;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getEmploymentStatus() {
        return employmentStatus;
    }

    public void setEmploymentStatus(String employmentStatus) {
        this.employmentStatus = employmentStatus;
    }

    public String getManagerId() {
        return managerId;
    }

    public void setManagerId(String managerId) {
        this.managerId = managerId;
    }

    public Boolean getIsActive() {
        return isActive;
    }

    public void setIsActive(Boolean isActive) {
        this.isActive = isActive;
    }

    public List<String> getSkills() {
        return skills;
    }

    public void setSkills(List<String> skills) {
        this.skills = skills;
    }

    public Address getAddress() {
        return address;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    public EmergencyContact getEmergencyContact() {
        return emergencyContact;
    }

    public void setEmergencyContact(EmergencyContact emergencyContact) {
        this.emergencyContact = emergencyContact;
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
}
