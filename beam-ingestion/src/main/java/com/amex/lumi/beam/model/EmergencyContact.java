package com.amex.lumi.beam.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Objects;

/**
 * Emergency contact. Saved as JSONB; only the phone is encrypted.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EmergencyContact implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;
    private String relationship;
    private String phone;
    private String email;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRelationship() {
        return relationship;
    }

    public void setRelationship(String relationship) {
        this.relationship = relationship;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof EmergencyContact that)) {
            return false;
        }
        return Objects.equals(name, that.name)
                && Objects.equals(relationship, that.relationship)
                && Objects.equals(phone, that.phone)
                && Objects.equals(email, that.email);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, relationship, phone, email);
    }
}
