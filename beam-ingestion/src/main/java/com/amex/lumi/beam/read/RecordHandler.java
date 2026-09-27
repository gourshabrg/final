package com.amex.lumi.beam.read;

import com.amex.lumi.beam.model.EmployeeRecord;

/**
 * Receives each record in file order; reasons must not contain field values.
 */
public interface RecordHandler {

    void onRecord(EmployeeRecord employee, Long sourceRecordNumber);

    void onError(String reason, Long sourceRecordNumber);

    /** The rest of the file cannot be read (e.g. broken JSON array); records already sent stay valid. */
    void onFileError(String reason);
}
