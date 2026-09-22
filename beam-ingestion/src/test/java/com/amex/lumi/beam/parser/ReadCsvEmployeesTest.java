package com.amex.lumi.beam.parser;

import com.amex.lumi.beam.model.EmergencyContact;
import com.amex.lumi.beam.model.EmployeeRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.KV;
import org.junit.jupiter.api.Test;
import com.amex.lumi.beam.model.Address;



import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

class ReadCsvEmployeesTest {

    @Test
    void shouldReadAndParseCsvEmployees() throws Exception {

        Path csvFile = Files.createTempFile(
                "employees-test-",
                ".csv"
        );

        String csv = """
                employee_id,first_name,last_name,email,phone_number,hire_date,department,job_title,salary,currency,employment_status,manager_id,is_active,skills,address_street,address_city,address_state,address_postal_code,address_country,emergency_contact_name,emergency_contact_relationship,emergency_contact_phone,emergency_contact_email
                EMP0001,Ravi,Kumar,ravi@example.com,9876543210,2025-01-15,Engineering,Software Engineer,75000,USD,ACTIVE,MGR001,true,"Java;Spring Boot;PostgreSQL",123 Main Street,Indore,MP,452001,India,Anita Kumar,Sister,9123456789,anita@example.com
                EMP0002,Amit,Sharma,amit@example.com,9876543211,2024-05-10,Engineering,Developer,80000,USD,ACTIVE,MGR002,true,"Java;Docker",456 Park Road,Bhopal,MP,462001,India,Rahul Sharma,Brother,9123456790,rahul@example.com
                """;

        Files.writeString(csvFile, csv);

        Pipeline pipeline = Pipeline.create();

        var employees = pipeline.apply(
                new ReadCsvEmployees(csvFile.toString())
        );

        PAssert.that(employees)
                .containsInAnyOrder(
                        KV.of(
                                1L,
                                createFirstEmployee()
                        ),
                        KV.of(
                                2L,
                                createSecondEmployee()
                        )
                );

        pipeline.run().waitUntilFinish();

        Files.deleteIfExists(csvFile);
    }

    private EmployeeRecord createFirstEmployee() {

        EmployeeRecord employee = new EmployeeRecord();

        employee.setEmployeeId("EMP0001");
        employee.setFirstName("Ravi");
        employee.setLastName("Kumar");
        employee.setEmail("ravi@example.com");
        employee.setPhoneNumber("9876543210");
        employee.setHireDate("2025-01-15");
        employee.setDepartment("Engineering");
        employee.setJobTitle("Software Engineer");
        employee.setSalary(75000L);
        employee.setCurrency("USD");
        employee.setEmploymentStatus("ACTIVE");
        employee.setManagerId("MGR001");
        employee.setIsActive(true);

        Address address = new Address();

address.setStreet("123 Main Street");
address.setCity("Indore");
address.setState("MP");
address.setPostalCode("452001");
address.setCountry("India");

employee.setAddress(address);

EmergencyContact emergencyContact =
        new EmergencyContact();

emergencyContact.setName("Anita Kumar");
emergencyContact.setRelationship("Sister");
emergencyContact.setPhone("9123456789");
emergencyContact.setEmail("anita@example.com");

employee.setEmergencyContact(emergencyContact);


        employee.setSkills(
                List.of(
                        "Java",
                        "Spring Boot",
                        "PostgreSQL"
                )
        );

        return employee;
    }

    private EmployeeRecord createSecondEmployee() {

    EmployeeRecord employee = new EmployeeRecord();

    employee.setEmployeeId("EMP0002");
    employee.setFirstName("Amit");
    employee.setLastName("Sharma");
    employee.setEmail("amit@example.com");
    employee.setPhoneNumber("9876543211");
    employee.setHireDate("2024-05-10");
    employee.setDepartment("Engineering");
    employee.setJobTitle("Developer");
    employee.setSalary(80000L);
    employee.setCurrency("USD");
    employee.setEmploymentStatus("ACTIVE");
    employee.setManagerId("MGR002");
    employee.setIsActive(true);

    employee.setSkills(
            List.of(
                    "Java",
                    "Docker"
            )
    );

    Address address = new Address();

    address.setStreet("456 Park Road");
    address.setCity("Bhopal");
    address.setState("MP");
    address.setPostalCode("462001");
    address.setCountry("India");

    employee.setAddress(address);

    EmergencyContact emergencyContact =
            new EmergencyContact();

    emergencyContact.setName("Rahul Sharma");
    emergencyContact.setRelationship("Brother");
    emergencyContact.setPhone("9123456790");
    emergencyContact.setEmail("rahul@example.com");

    employee.setEmergencyContact(emergencyContact);

    return employee;
}

}
