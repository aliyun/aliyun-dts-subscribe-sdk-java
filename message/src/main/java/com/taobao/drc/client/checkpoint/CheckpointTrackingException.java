package com.taobao.drc.client.checkpoint;

/** Terminal for this subscription run: retrying unchanged settings can repeat the failure. */
public final class CheckpointTrackingException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    public CheckpointTrackingException(String message) { super(message); }
}
