package com.amex.lumi.beam.transform;

import com.amex.lumi.beam.common.PipelineConstants;
import com.amex.lumi.beam.model.Address;
import com.amex.lumi.beam.model.EmergencyContact;
import com.amex.lumi.beam.model.EmployeeRecord;

import java.util.List;

/**
 * Empty text values become a space (not salary, is_active or hire_date).
 */
public final class MissingValueCleanser {

    private MissingValueCleanser() {
    }

    public static EmployeeRecord cleanse(EmployeeRecord source) {
        return source.toBuilder()
                .employeeId(fill(source.getEmployeeId()))
                .firstName(fill(source.getFirstName()))
                .lastName(fill(source.getLastName()))
                .email(fill(source.getEmail()))
                .phoneNumber(fill(source.getPhoneNumber()))
                .department(fill(source.getDepartment()))
                .jobTitle(fill(source.getJobTitle()))
                .currency(fill(source.getCurrency()))
                .employmentStatus(fill(source.getEmploymentStatus()))
                .managerId(fill(source.getManagerId()))
                .skills(source.getSkills() == null ? List.of() : source.getSkills())
                .address(cleanse(source.getAddress()))
                .emergencyContact(cleanse(source.getEmergencyContact()))
                .build();
    }

    private static Address cleanse(Address source) {
        Address original = source == null ? Address.builder().build() : source;
        return Address.builder()
                .street(fill(original.getStreet()))
                .city(fill(original.getCity()))
                .state(fill(original.getState()))
                .postalCode(fill(original.getPostalCode()))
                .country(fill(original.getCountry()))
                .build();
    }

    private static EmergencyContact cleanse(EmergencyContact source) {
        EmergencyContact original = source == null ? EmergencyContact.builder().build() : source;
        return EmergencyContact.builder()
                .name(fill(original.getName()))
                .relationship(fill(original.getRelationship()))
                .phone(fill(original.getPhone()))
                .email(fill(original.getEmail()))
                .build();
    }

    private static String fill(String value) {
        return value == null || value.isEmpty() ? PipelineConstants.MISSING_VALUE : value;
    }
}
