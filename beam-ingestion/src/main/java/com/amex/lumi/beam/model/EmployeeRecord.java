package com.amex.lumi.beam.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

import java.util.List;

/**
 * Canonical employee payload shared by the ingestion readers and Beam stages.
 */
public class EmployeeRecord implements Serializable {

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

    public EmployeeRecord() {
    }

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

    public void setIsActive(Boolean active) {
        isActive = active;
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

    public void setEmergencyContact(
            EmergencyContact emergencyContact) {

        this.emergencyContact = emergencyContact;
    }

    @Override
public boolean equals(Object o) {
    if (this == o) {
        return true;
    }

    if (!(o instanceof EmployeeRecord that)) {
        return false;
    }

    return java.util.Objects.equals(employeeId, that.employeeId)
            && java.util.Objects.equals(firstName, that.firstName)
            && java.util.Objects.equals(lastName, that.lastName)
            && java.util.Objects.equals(email, that.email)
            && java.util.Objects.equals(phoneNumber, that.phoneNumber)
            && java.util.Objects.equals(hireDate, that.hireDate)
            && java.util.Objects.equals(department, that.department)
            && java.util.Objects.equals(jobTitle, that.jobTitle)
            && java.util.Objects.equals(salary, that.salary)
            && java.util.Objects.equals(currency, that.currency)
            && java.util.Objects.equals(employmentStatus, that.employmentStatus)
            && java.util.Objects.equals(managerId, that.managerId)
            && java.util.Objects.equals(isActive, that.isActive)
            && java.util.Objects.equals(skills, that.skills)
            && java.util.Objects.equals(address, that.address)
            && java.util.Objects.equals(emergencyContact, that.emergencyContact);
}

@Override
public int hashCode() {
    return java.util.Objects.hash(
            employeeId,
            firstName,
            lastName,
            email,
            phoneNumber,
            hireDate,
            department,
            jobTitle,
            salary,
            currency,
            employmentStatus,
            managerId,
            isActive,
            skills,
            address,
            emergencyContact
    );
}

}
