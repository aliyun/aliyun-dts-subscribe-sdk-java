package com.taobao.drc.client.checkpoint;

/** The SDK cannot prove that the available position is valid for the new channel/topology. */
public final class UnsafeRecoveryException extends IllegalStateException {
    private static final long serialVersionUID = 1L;
    public UnsafeRecoveryException(String message) { super(message); }
}
