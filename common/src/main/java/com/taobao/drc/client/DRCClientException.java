package com.taobao.drc.client;

/**
 * Describes special exceptions produced in the @see DRCClient.  
 */
public class DRCClientException extends RuntimeException {
    
    private static final long serialVersionUID = 1L;

    public DRCClientException() {}

    public DRCClientException(final String message) {
        super(message);
    }
}