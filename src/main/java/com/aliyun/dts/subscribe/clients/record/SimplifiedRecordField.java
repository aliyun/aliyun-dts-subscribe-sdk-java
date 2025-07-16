package com.aliyun.dts.subscribe.clients.record;

import com.aliyun.dts.subscribe.clients.record.impl.RawDataType;
import com.aliyun.dts.subscribe.clients.record.value.Value;

import java.util.Collections;
import java.util.Set;

public class SimplifiedRecordField implements RecordField {

    private String fieldName;
    private final int rawDataTypeNum;
    private boolean isPrimaryKey;
    private boolean isUniqueKey;

    private int fieldPosition;

    private RawDataType rawDataType;

    public SimplifiedRecordField(String fieldName, int rawDataTypeNum) {
        this.fieldName = fieldName;
        this.rawDataTypeNum = rawDataTypeNum;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public Set<String> getAliases() {
        return Collections.emptySet();
    }

    @Override
    public RawDataType getRawDataType() {
        return rawDataType;
    }

    public int getRawDataTypeNum() {
        return rawDataTypeNum;
    }

    public Value getDefaultValue() {
        return null;
    }

    public boolean isNullable() {
        return true;
    }

    public boolean isUnique() {
        return isUniqueKey;
    }

    public RecordField setUnique(boolean isUnique) {
        isUniqueKey = isUnique;
        return this;
    }

    public boolean isPrimary() {
        return isPrimaryKey;
    }

    public boolean setPrimary(boolean isPrimary) {
        isPrimaryKey = isPrimary;
        return isPrimaryKey;
    }

    public boolean isIndexed() {
        return isPrimaryKey || isUniqueKey;
    }

    public boolean isAutoIncrement() {
        return false;
    }

    public int keySeq() {
        return 0;
    }

    public int getFieldPosition() {
        return fieldPosition;
    }

    public void setFieldPosition(int fieldPosition) {
        this.fieldPosition = fieldPosition;
    }

    public int getScale() {
        return 0;
    }

    @Override
    public String toString() {
        return "{" +
                "fieldName='" + fieldName + '\'' +
                ", rawDataTypeNum=" + rawDataTypeNum +
                ", isPrimaryKey=" + isPrimaryKey +
                ", isUniqueKey=" + isUniqueKey +
                ", fieldPosition=" + fieldPosition +
                '}';
    }
}
