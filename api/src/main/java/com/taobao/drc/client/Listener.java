package com.taobao.drc.client;

import com.taobao.drc.client.message.DataMessage;

/**
 * Listeners are notified when a message comes. Users need implement the
 * interface and add it to @see DRCClient.
 */
public interface Listener {

    /**
     * Message is notified to the listener.
     * @param message
     */
    public void notify(DataMessage message) throws Exception;

    /**
     * Message
     */
    @Deprecated
    public void notifyRuntimeLog(final String level, final String log) throws Exception;

    /**
     * Exceptions thrown out by @see notify is handled.
     * @param e is the handled exception.
     */
    @Deprecated
    public void handleException(Exception e);
}
