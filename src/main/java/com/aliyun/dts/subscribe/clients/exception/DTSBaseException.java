package com.aliyun.dts.subscribe.clients.exception;

public class DTSBaseException extends RuntimeException {

    public DTSBaseException(String message) {
        super(message);
    }

    public DTSBaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public DTSBaseException(Throwable throwable) {
        super(throwable);
    }

    public DTSBaseException() {
        super();
    }
}