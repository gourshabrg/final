package com.amex.lumi.ingestion.controller;

import com.amex.lumi.ingestion.dto.EmployeeResponse;
import com.amex.lumi.ingestion.service.EmployeeService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/employees")
public class EmployeeController {

    private final EmployeeService employeeService;

    public EmployeeController(EmployeeService employeeService) {
        this.employeeService = employeeService;
    }

    /** Employee with phone, salary and emergency phone decrypted. */
    @GetMapping("/{employeeId}")
    public EmployeeResponse getEmployee(@PathVariable String employeeId) {
        return employeeService.getDecryptedEmployee(employeeId);
    }
}
