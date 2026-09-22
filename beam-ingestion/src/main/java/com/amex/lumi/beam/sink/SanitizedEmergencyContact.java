package com.amex.lumi.beam.sink;

import com.amex.lumi.beam.model.EmergencyContact;

import java.io.Serializable;

public class SanitizedEmergencyContact
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final String relationship;
    private final String phone;
    private final String email;

    private SanitizedEmergencyContact(
            String name,
            String relationship,
            String phone,
            String email) {

        this.name = name;
        this.relationship = relationship;
        this.phone = phone;
        this.email = email;
    }

    public static SanitizedEmergencyContact from(
            EmergencyContact contact) {

        return new SanitizedEmergencyContact(
                contact.getName(),
                contact.getRelationship(),
                contact.getPhone() == null
                        ? null
                        : "[REDACTED]",
                contact.getEmail()
        );
    }

    public String getName() {
        return name;
    }

    public String getRelationship() {
        return relationship;
    }

    public String getPhone() {
        return phone;
    }

    public String getEmail() {
        return email;
    }
}
