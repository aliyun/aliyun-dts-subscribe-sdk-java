package com.taobao.drc.client.utils;

import com.taobao.drc.client.Listener;
import com.taobao.drc.client.network.ConnectionStateChangeListener;
import com.taobao.drc.client.network.NetworkEndpoint;
import io.netty.util.AttributeKey;

public class NetworkConstant {

    public static final AttributeKey<NetworkEndpoint> networkKey=AttributeKey.valueOf("network");

    public static final AttributeKey<Listener> listenerKey = AttributeKey.valueOf("listener");

    public static final AttributeKey<ConnectionStateChangeListener> CONNECTION_STATE_CHANGE_LISTENER_ATTRIBUTE_KEY
            = AttributeKey.valueOf("connectionStateListener");

}
