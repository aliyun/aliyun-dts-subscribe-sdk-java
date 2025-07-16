package com.aliyun.dts.subscribe.clients.record.impl;

public interface RawDataType {

    String getTypeName();

    int getTypeId();

    boolean isLobType();
}

