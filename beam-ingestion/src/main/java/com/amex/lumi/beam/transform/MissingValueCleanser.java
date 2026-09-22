package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.common.PipelineConstants;
import com.amex.lumi.beam.model.Address;
import com.amex.lumi.beam.model.EmergencyContact;
import com.amex.lumi.beam.model.EmployeeRecord;

import java.util.List;

/**
 * Phase 1 rule: a missing (null or empty) text value becomes a single space.
 * Salary, is_active and hire_date are skipped because number, true/false and date columns cannot hold a space.
 * Returns a new copy, because Beam does not allow changing the record it gave us.
 */
public final class MissingValueCleanser {

    private MissingValueCleanser() {
    }

    public static EmployeeRecord cleanse(EmployeeRecord source) {
        EmployeeRecord copy = new EmployeeRecord();
        copy.setEmployeeId(fill(source.getEmployeeId()));
        copy.setFirstName(fill(source.getFirstName()));
        copy.setLastName(fill(source.getLastName()));
        copy.setEmail(fill(source.getEmail()));
        copy.setPhoneNumber(fill(source.getPhoneNumber()));
        copy.setHireDate(source.getHireDate());
        copy.setDepartment(fill(source.getDepartment()));
        copy.setJobTitle(fill(source.getJobTitle()));
        copy.setSalary(source.getSalary());
        copy.setCurrency(fill(source.getCurrency()));
        copy.setEmploymentStatus(fill(source.getEmploymentStatus()));
        copy.setManagerId(fill(source.getManagerId()));
        copy.setIsActive(source.getIsActive());
        copy.setSkills(source.getSkills() == null ? List.of() : List.copyOf(source.getSkills()));
        copy.setAddress(cleanse(source.getAddress()));
        copy.setEmergencyContact(cleanse(source.getEmergencyContact()));
        return copy;
    }

    private static Address cleanse(Address source) {
        Address original = source == null ? new Address() : source;
        Address copy = new Address();
        copy.setStreet(fill(original.getStreet()));
        copy.setCity(fill(original.getCity()));
        copy.setState(fill(original.getState()));
        copy.setPostalCode(fill(original.getPostalCode()));
        copy.setCountry(fill(original.getCountry()));
        return copy;
    }

    private static EmergencyContact cleanse(EmergencyContact source) {
        EmergencyContact original = source == null ? new EmergencyContact() : source;
        EmergencyContact copy = new EmergencyContact();
        copy.setName(fill(original.getName()));
        copy.setRelationship(fill(original.getRelationship()));
        copy.setPhone(fill(original.getPhone()));
        copy.setEmail(fill(original.getEmail()));
        return copy;
    }

    private static String fill(String value) {
        return value == null || value.isEmpty() ? PipelineConstants.MISSING_VALUE : value;
    }
}
