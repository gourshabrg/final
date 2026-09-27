package com.amex.lumi.beam.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import java.io.Serializable;
import java.util.Objects;

/**
 * Employee address. Saved as JSONB. Immutable: create it with {@link #builder()}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(builder = Address.Builder.class)
public final class Address implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String street;
    private final String city;
    private final String state;
    @JsonProperty("postal_code")
    private final String postalCode;
    private final String country;

    private Address(Builder builder) {
        this.street = builder.street;
        this.city = builder.city;
        this.state = builder.state;
        this.postalCode = builder.postalCode;
        this.country = builder.country;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder().street(street).city(city).state(state).postalCode(postalCode).country(country);
    }

    public String getStreet() {
        return street;
    }

    public String getCity() {
        return city;
    }

    public String getState() {
        return state;
    }

    public String getPostalCode() {
        return postalCode;
    }

    public String getCountry() {
        return country;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Address that)) {
            return false;
        }
        return Objects.equals(street, that.street)
                && Objects.equals(city, that.city)
                && Objects.equals(state, that.state)
                && Objects.equals(postalCode, that.postalCode)
                && Objects.equals(country, that.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(street, city, state, postalCode, country);
    }

    /** Also used by Jackson to read JSON. */
    @JsonPOJOBuilder(withPrefix = "")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Builder {
        private String street;
        private String city;
        private String state;
        private String postalCode;
        private String country;

        public Builder street(String value) {
            this.street = value;
            return this;
        }

        public Builder city(String value) {
            this.city = value;
            return this;
        }

        public Builder state(String value) {
            this.state = value;
            return this;
        }

        @JsonProperty("postal_code")
        public Builder postalCode(String value) {
            this.postalCode = value;
            return this;
        }

        public Builder country(String value) {
            this.country = value;
            return this;
        }

        public Address build() {
            return new Address(this);
        }
    }
}
