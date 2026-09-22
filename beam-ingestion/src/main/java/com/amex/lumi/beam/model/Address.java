package com.amex.lumi.beam.model;
import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Employee address value object mapped from nested input fields.
 */
public class Address implements Serializable {

    private String street;

    private String city;

    private String state;

    @JsonProperty("postal_code")
    private String postalCode;

    private String country;

    public Address() {
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getPostalCode() {
        return postalCode;
    }

    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }
    @Override
public boolean equals(Object o) {
    if (this == o) {
        return true;
    }

    if (!(o instanceof Address address)) {
        return false;
    }

    return java.util.Objects.equals(street, address.street)
            && java.util.Objects.equals(city, address.city)
            && java.util.Objects.equals(state, address.state)
            && java.util.Objects.equals(postalCode, address.postalCode)
            && java.util.Objects.equals(country, address.country);
}

@Override
public int hashCode() {
    return java.util.Objects.hash(
            street,
            city,
            state,
            postalCode,
            country
    );
}

}
