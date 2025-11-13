package com.aliyun.dts.subscribe.clients.record.impl;

public class DefaultRawDataType implements RawDataType {

    private final String typeName;
    private final int typeId;
    private final boolean isLobType;

    DefaultRawDataType(String typeName, int typeId, boolean isLobType) {
        this.typeName = typeName;
        this.typeId = typeId;
        this.isLobType = isLobType;
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

    public static RawDataType of(int dataTypeId) {
        return of("UNKNOWN", dataTypeId);
    }

    public static RawDataType of(String dataTypeName, int dataTypeId) {
        return of(dataTypeName, dataTypeId, false);
    }

    public static RawDataType of(String dataTypeName, int dataTypeId, boolean isLobType) {
        return new DefaultRawDataType(dataTypeName, dataTypeId, isLobType);
    }

    public static RawDataType copy(RawDataType source) {
        return DefaultRawDataType.of(source.getTypeName(), source.getTypeId(), source.isLobType());
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
            .append(" typeId:").append(typeId).append(",")
            .append(" isLobType:").append(isLobType).append(",")
            .append("}");

        return sbl.toString();
    }
}
