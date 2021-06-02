package com.aliyun.dts.subscribe.clients.exception;

public class CriticalException  extends DTSBaseException {
    public CriticalException(String errMsg) {
        super(errMsg);
    }

    public CriticalException(String message, Throwable cause) {
        super(message, cause);
    }
}
