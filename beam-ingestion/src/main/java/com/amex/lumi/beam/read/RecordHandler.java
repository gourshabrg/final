package com.amex.lumi.beam.read;

import com.amex.lumi.beam.model.EmployeeRecord;

/**
 * Receives each record a parser finds, in file order.
 */
public interface RecordHandler {

    void onRecord(EmployeeRecord employee);

    /** The reason must not contain field values (it is logged). */
    void onError(String reason);
}
