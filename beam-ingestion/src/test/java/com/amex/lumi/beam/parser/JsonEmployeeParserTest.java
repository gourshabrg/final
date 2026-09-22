package com.amex.lumi.beam.parser;

import com.amex.lumi.beam.model.EmployeeRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


class JsonEmployeeParserTest {


    @Test
    void shouldParseEmployeeJsonArray() throws Exception {

        String json = """
                [
                  {
                    "employee_id": "EMPLOYEE_TOO_LONG_001",
                    "first_name": "Arjun",
                    "last_name": "Sharma",
                    "email": "arjun.sharma1@tc.com",
                    "phone_number": "9876500000",
                    "hire_date": "2019-01-01",
                    "department": "Engineering",
                    "job_title": "Senior Backend Engineer",
                    "salary": 650000,
                    "currency": "INR",
                    "employment_status": "Full-time",
                    "manager_id": "M000001",
                    "is_active": true,
                    "skills": [
                      "Java",
                      "Spring Boot",
                      "PostgreSQL",
                      "Docker"
                    ],
                    "address": {
                      "street": "100, Tech Park Avenue, Sector 10",
                      "city": "Bengaluru",
                      "state": "Karnataka",
                      "postal_code": "560100",
                      "country": "India"
                    },
                    "emergency_contact": {
                      "name": "Karan Sharma",
                      "relationship": "Spouse",
                      "phone": "9986500000",
                      "email": "emergency1@example.com"
                    }
                  },
                  {
                    "employee_id": "E000002",
                    "first_name": "A",
                    "last_name": "Singh",
                    "email": "rahul.singh2@tc.com",
                    "phone_number": "9876500001",
                    "hire_date": "2020-02-02",
                    "department": "Platform",
                    "job_title": "Backend Engineer",
                    "salary": 700000,
                    "currency": "USD",
                    "employment_status": "Contract",
                    "manager_id": "M000002",
                    "is_active": true,
                    "skills": [
                      "Java",
                      "Spring Boot",
                      "MySQL",
                      "AWS"
                    ],
                    "address": {
                      "street": "101, Tech Park Avenue, Sector 11",
                      "city": "Hyderabad",
                      "state": "Telangana",
                      "postal_code": "500081",
                      "country": "India"
                    },
                    "emergency_contact": {
                      "name": "Aditya Singh",
                      "relationship": "Spouse",
                      "phone": "9986500001",
                      "email": "emergency2@example.com"
                    }
                  }
                ]
                """;


        ObjectMapper objectMapper =
                new ObjectMapper();


        JsonEmployeeParser parser =
                new JsonEmployeeParser(
                        objectMapper
                );


        List<EmployeeRecord> employees =
                parser.parse(json);


        assertNotNull(employees);

        assertEquals(
                2,
                employees.size()
        );


        EmployeeRecord firstEmployee =
                employees.get(0);


        assertEquals(
                "EMPLOYEE_TOO_LONG_001",
                firstEmployee.getEmployeeId()
        );

        assertEquals(
                "Arjun",
                firstEmployee.getFirstName()
        );

        assertEquals(
                "Sharma",
                firstEmployee.getLastName()
        );

        assertEquals(
                "arjun.sharma1@tc.com",
                firstEmployee.getEmail()
        );

        assertEquals(
                "9876500000",
                firstEmployee.getPhoneNumber()
        );

        assertEquals(
                650000L,
                firstEmployee.getSalary()
        );

        assertTrue(
                firstEmployee.getIsActive()
        );


        assertNotNull(
                firstEmployee.getSkills()
        );

        assertEquals(
                4,
                firstEmployee.getSkills().size()
        );


        assertNotNull(
                firstEmployee.getAddress()
        );

        assertEquals(
                "Bengaluru",
                firstEmployee.getAddress().getCity()
        );

        assertEquals(
                "560100",
                firstEmployee.getAddress().getPostalCode()
        );


        assertNotNull(
                firstEmployee.getEmergencyContact()
        );

        assertEquals(
                "Karan Sharma",
                firstEmployee
                        .getEmergencyContact()
                        .getName()
        );

        assertEquals(
                "9986500000",
                firstEmployee
                        .getEmergencyContact()
                        .getPhone()
        );


        EmployeeRecord secondEmployee =
                employees.get(1);


        assertEquals(
                "E000002",
                secondEmployee.getEmployeeId()
        );

        assertEquals(
                "Hyderabad",
                secondEmployee
                        .getAddress()
                        .getCity()
        );
    }


    @Test
    void shouldParseSingleEmployeeJsonRecord() throws Exception {

        String json = """
                {
                  "employee_id": "E000003",
                  "first_name": "Vikram",
                  "last_name": "Patel",
                  "email": "vikram.patel@example.com",
                  "phone_number": "9876500002",
                  "hire_date": "2021-03-15",
                  "department": "Engineering",
                  "job_title": "Backend Engineer",
                  "salary": 800000,
                  "currency": "INR",
                  "employment_status": "Full-time",
                  "manager_id": "M000003",
                  "is_active": true,
                  "skills": [
                    "Java",
                    "Spring Boot",
                    "PostgreSQL"
                  ],
                  "address": {
                    "street": "200 Tech Park Road",
                    "city": "Pune",
                    "state": "Maharashtra",
                    "postal_code": "411001",
                    "country": "India"
                  },
                  "emergency_contact": {
                    "name": "Rohit Patel",
                    "relationship": "Brother",
                    "phone": "9986500002",
                    "email": "rohit@example.com"
                  }
                }
                """;


        ObjectMapper objectMapper =
                new ObjectMapper();


        JsonEmployeeParser parser =
                new JsonEmployeeParser(
                        objectMapper
                );


        EmployeeRecord employee =
                parser.parseRecord(json);


        assertNotNull(employee);


        assertEquals(
                "E000003",
                employee.getEmployeeId()
        );

        assertEquals(
                "Vikram",
                employee.getFirstName()
        );

        assertEquals(
                "Patel",
                employee.getLastName()
        );

        assertEquals(
                "vikram.patel@example.com",
                employee.getEmail()
        );

        assertEquals(
                "9876500002",
                employee.getPhoneNumber()
        );

        assertEquals(
                800000L,
                employee.getSalary()
        );

        assertTrue(
                employee.getIsActive()
        );


        assertNotNull(
                employee.getSkills()
        );

        assertEquals(
                3,
                employee.getSkills().size()
        );


        assertNotNull(
                employee.getAddress()
        );

        assertEquals(
                "Pune",
                employee
                        .getAddress()
                        .getCity()
        );

        assertEquals(
                "411001",
                employee
                        .getAddress()
                        .getPostalCode()
        );


        assertNotNull(
                employee.getEmergencyContact()
        );

        assertEquals(
                "Rohit Patel",
                employee
                        .getEmergencyContact()
                        .getName()
        );

        assertEquals(
                "Brother",
                employee
                        .getEmergencyContact()
                        .getRelationship()
        );

        assertEquals(
                "9986500002",
                employee
                        .getEmergencyContact()
                        .getPhone()
        );

        assertEquals(
                "rohit@example.com",
                employee
                        .getEmergencyContact()
                        .getEmail()
        );
    }
}
