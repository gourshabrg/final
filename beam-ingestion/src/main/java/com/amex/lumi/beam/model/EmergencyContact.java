package com.amex.lumi.beam.model;
import java.io.Serializable;

/**
 * Employee emergency-contact value object mapped from nested input fields.
 */
public class EmergencyContact implements Serializable {

    private String name;

    private String relationship;

    private String phone;

    private String email;

    public EmergencyContact() {
    }

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
public boolean equals(Object o) {
    if (this == o) {
        return true;
    }

    if (!(o instanceof EmergencyContact that)) {
        return false;
    }

    return java.util.Objects.equals(name, that.name)
            && java.util.Objects.equals(relationship, that.relationship)
            && java.util.Objects.equals(phone, that.phone)
            && java.util.Objects.equals(email, that.email);
}

@Override
public int hashCode() {
    return java.util.Objects.hash(
            name,
            relationship,
            phone,
            email
    );
}

}
