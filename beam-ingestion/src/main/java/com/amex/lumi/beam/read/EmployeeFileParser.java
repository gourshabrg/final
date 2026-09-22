package com.amex.lumi.beam.read;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;

/**
 * One parser per file format. Serializable because Beam copies it to every worker thread.
 */
public interface EmployeeFileParser extends Serializable {

    /** Throws IOException only when the whole file is unreadable; bad records go to onError. */
    void parse(Reader reader, RecordHandler handler) throws IOException;
}
