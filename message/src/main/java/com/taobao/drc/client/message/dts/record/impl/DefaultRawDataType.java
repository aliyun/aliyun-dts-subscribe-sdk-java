package com.taobao.drc.client.message.dts.record.impl;

import com.taobao.drc.client.message.dts.record.RawDataType;

public class DefaultRawDataType implements RawDataType {

    private final String typeName;
    private final int typeId;
    private final boolean isLobType;
    private final String encoding;

    DefaultRawDataType(String typeName, int typeId, boolean isLobType, String encoding) {
        this.typeName = typeName;
        this.typeId = typeId;
        this.isLobType = isLobType;
        this.encoding = encoding;
    }

    @Override
    public String getTypeName() {
        return typeName;
    }

    @Override
    public int getTypeId() {
        return typeId;
    }

    @Override
    public boolean isLobType() {
        return isLobType;
    }

    @Override
    public String getEncoding() {
        return encoding;
    }

    public static RawDataType of(int dataTypeId) {
        return of("UNKNOWN", dataTypeId);
    }

    public static RawDataType of(String dataTypeName, int dataTypeId) {
        return of(dataTypeName, dataTypeId, false);
    }

    public static RawDataType of(String dataTypeName, int dataTypeId, boolean isLobType) {
        return new DefaultRawDataType(dataTypeName, dataTypeId, isLobType, "utf-8");
    }

    public static RawDataType of(String dataTypeName, int dataTypeId, boolean isLobType, String encoding) {
        return new DefaultRawDataType(dataTypeName, dataTypeId, isLobType, encoding);
    }

    public static RawDataType copy(RawDataType source, String encoding) {
        return DefaultRawDataType.of(source.getTypeName(), source.getTypeId(), source.isLobType(), encoding);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object another) {
        if (another instanceof DefaultRawDataType) {
            if (typeId == ((DefaultRawDataType) another).typeId) {
                return true;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        StringBuilder sbl = new StringBuilder();

        sbl.append("{")
            .append("typeName:").append(typeName).append(",")
            .append("typeId").append(typeId).append(",")
            .append("isLobType").append(isLobType).append(",")
            .append("encoding").append(encoding)
            .append("}");

        return sbl.toString();
    }
}
