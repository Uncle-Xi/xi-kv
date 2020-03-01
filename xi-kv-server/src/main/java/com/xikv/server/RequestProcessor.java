package com.xikv.server;

public interface RequestProcessor {

    class RequestProcessorException extends Exception {
        public RequestProcessorException(String msg, Throwable t) {
            super(msg, t);
        }
    }

    void processRequest(Record record) throws RequestProcessorException;
}
