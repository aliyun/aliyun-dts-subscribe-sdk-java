package com.taobao.drc.client.network.congestion;

/**
 * @author yangyang
 * @since 17/1/19
 */
public interface CongestionController {
    /**
     * Called when a new record is produced, decide whether the producing should be stopped (if producing is ongoing).
     *
     * @return True if the producing should be stopped, false otherwise.
     */
    boolean produced();

    /**
     * Called when a record is consumed, decide whether the producing should be resumed (if producing is stopped).
     * @return True if the producing should be resumed, false otherwise.
     */
    boolean consumed();

    /**
     * @return the number of records that are produced but not yet consumed.
     */
    long pendingNum();
}
