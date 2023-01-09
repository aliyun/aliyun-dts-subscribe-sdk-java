package com.taobao.drc.client.message.dts.record;

public interface RawDataType {

    String getTypeName();

    int getTypeId();

    boolean isLobType();

    String getEncoding();
}
