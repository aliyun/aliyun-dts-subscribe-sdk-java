package com.taobao.drc.client.network;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

/**
 * @author yangyang
 * @since 17/6/12
 */
public interface ConnectionStateChangeListener {
    void onChannelActive(Channel channel);

    void onFirstRecord(Channel channel);

    long onChannelInactive(Channel channel);

    long onException(Channel channel, Throwable throwable);

    public static final AttributeKey<ConnectionStateChangeListener> CONNECTION_STATE_CHANGE_LISTENER_ATTRIBUTE_KEY
            = AttributeKey.valueOf("connectionStateListener");
}
