package com.amex.lumi.beam.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import java.io.Serializable;
import java.util.Objects;

/**
 * Emergency contact, saved as JSONB; only the phone is encrypted.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = EmergencyContact.Builder.class)
public final class EmergencyContact implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final String relationship;
    private final String phone;
    private final String email;

    private EmergencyContact(Builder builder) {
        this.name = builder.name;
        this.relationship = builder.relationship;
        this.phone = builder.phone;
        this.email = builder.email;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder().name(name).relationship(relationship).phone(phone).email(email);
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

    /** Also used by Jackson to read JSON. */
    @JsonPOJOBuilder(withPrefix = "")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Builder {
        private String name;
        private String relationship;
        private String phone;
        private String email;

        public Builder name(String value) {
            this.name = value;
            return this;
        }

        public Builder relationship(String value) {
            this.relationship = value;
            return this;
        }

        public Builder phone(String value) {
            this.phone = value;
            return this;
        }

        public Builder email(String value) {
            this.email = value;
            return this;
        }

        public EmergencyContact build() {
            return new EmergencyContact(this);
        }
    }
}
