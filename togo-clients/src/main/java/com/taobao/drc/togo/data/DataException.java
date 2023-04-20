package com.taobao.drc.togo.data;

/**
 * @author yangyang
 * @since 17/3/22
 */
public class DataException extends RuntimeException {
    public DataException(String message) {
        super(message);
    }

    public DataException(String message, Throwable cause) {
        super(message, cause);
    }
}
