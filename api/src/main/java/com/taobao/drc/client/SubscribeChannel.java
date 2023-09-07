package com.taobao.drc.client;

public enum SubscribeChannel {
    DTS, DRC;
    public static SubscribeChannel valueBy(String key) {
        try {
            return SubscribeChannel.valueOf(key.toUpperCase());
        } catch (Throwable e) {
            return DRC;
        }
    }
}
