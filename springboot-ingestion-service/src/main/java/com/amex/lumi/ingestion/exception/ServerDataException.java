package com.amex.lumi.ingestion.exception;

/**
 * Our stored data or files could not be read; not the client's fault, so 500.
 */
public class ServerDataException extends LumiException {

    public ServerDataException(String message, Throwable cause) {
        super(ErrorCode.SERVER_DATA_ERROR, message, cause);
    }
}
