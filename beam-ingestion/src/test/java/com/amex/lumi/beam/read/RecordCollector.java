package com.amex.lumi.beam.read;

import com.amex.lumi.beam.model.EmployeeRecord;

import java.util.ArrayList;
import java.util.List;

/** Collects parser output in tests. */
class RecordCollector implements RecordHandler {

    final List<EmployeeRecord> records = new ArrayList<>();
    final List<String> errors = new ArrayList<>();
    final List<Long> recordNumbers = new ArrayList<>();
    final List<Long> errorNumbers = new ArrayList<>();
    final List<String> fileErrors = new ArrayList<>();

    @Override
    public void onRecord(EmployeeRecord employee, Long sourceRecordNumber) {
        records.add(employee);
        recordNumbers.add(sourceRecordNumber);
    }

    @Override
    public void onError(String reason, Long sourceRecordNumber) {
        errors.add(reason);
        errorNumbers.add(sourceRecordNumber);
    }

    @Override
    public void onFileError(String reason) {
        fileErrors.add(reason);
    }
}
