package com.amex.lumi.beam.read;

import com.amex.lumi.beam.model.EmployeeRecord;

import java.util.ArrayList;
import java.util.List;

/** Collects parser output in tests. */
class RecordCollector implements RecordHandler {

    final List<EmployeeRecord> records = new ArrayList<>();
    final List<String> errors = new ArrayList<>();

    @Override
    public void onRecord(EmployeeRecord employee) {
        records.add(employee);
    }

    @Override
    public void onError(String reason) {
        errors.add(reason);
    }
}
