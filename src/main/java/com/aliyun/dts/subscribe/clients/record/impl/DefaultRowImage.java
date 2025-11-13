package com.aliyun.dts.subscribe.clients.record.impl;

import com.aliyun.dts.subscribe.clients.common.NullableOptional;
import com.aliyun.dts.subscribe.clients.record.*;
import com.aliyun.dts.subscribe.clients.record.value.Value;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DefaultRowImage implements RowImage {

    private final RecordSchema recordSchema;
    private final Value[] values;
    private long size;

    public DefaultRowImage(RecordSchema recordSchema) {
        this.recordSchema = recordSchema;
        this.values = new Value[recordSchema.getFieldCount()];
    }

    public DefaultRowImage(RecordSchema recordSchema, int fieldCount) {
        this.recordSchema = recordSchema;
        this.values = new Value[fieldCount];
    }

    @Override
    public Value[] getValues() {
        return this.values;
    }

    @Override
    public Value getValue(int index) {
        return values[index];
    }

    @Override
    public Value getValue(RecordField field) {
        return getValue(field.getFieldPosition());
    }

    @Override
    public Value getValue(String fieldName) {
        NullableOptional<RecordField> recordField = recordSchema.getField(fieldName);
        return recordField.map(field -> getValue(field))
                .orElse(null);
    }

    private void accumulateSize(Value value) {
        if (null != value) {
            size += value.size();
        }
    }

    public void setValue(int i, Value value) {
        values[i] = value;

        accumulateSize(value);
    }

    public void setValue(String fieldName, Value value) {
        RecordField recordField = recordSchema.getField(fieldName)
                .orElse(null);
        setValue(recordField, value);
    }

    public void setValue(RecordField field, Value value) {
        int index = field.getFieldPosition();
        setValue(index, value);
    }

    @Override
    public Map<String, Value> toMap(Function<String, String> filedNameResolver, Function<Value, Value> valueResolver) {
        Map<String, Value> valueMap = new TreeMap<>();
        int i = 0;

        for (RecordField field : recordSchema.getFields()) {
            valueMap.put(filedNameResolver == null ? field.getFieldName() : filedNameResolver.apply(field.getFieldName()),
                    valueResolver == null ? values[i] : valueResolver.apply(values[i]));
            i++;
        }

        return valueMap;
    }

    public Pair<RecordField, Value>[] buildFieldValuePairArray(Collection<RecordField> recordFields) {
        Pair<RecordField, Value>[] rs = new ImmutablePair[recordFields.size()];
        int index = 0;
        for (RecordField recordField : recordFields) {
            rs[index] = Pair.of(recordField, getValue(recordField));
        }

        return rs;
    }

    @Override
    public Pair<RecordField, Value>[] getPrimaryKeyValues() {
        RecordIndexInfo recordIndexInfo = recordSchema.getPrimaryIndexInfo();
        if (null == recordIndexInfo) {
            return null;
        }

        return buildFieldValuePairArray(recordIndexInfo.getIndexFields());
    }

    private Pair<RecordField, Value>[] buildAllFieldValuePairArray(List<? extends RecordIndexInfo> recordIndexInfoList) {
        if (null == recordIndexInfoList || recordIndexInfoList.isEmpty()) {
            return null;
        }

        Set<RecordField> recordFieldSet = recordIndexInfoList.stream()
                .flatMap(indexInfo -> indexInfo.getIndexFields().stream())
                .collect(Collectors.toSet());

        return buildFieldValuePairArray(recordFieldSet);
    }

    @Override
    public Pair<RecordField, Value>[] getUniqueKeyValues() {
        List<RecordIndexInfo> recordIndexInfoList = recordSchema.getUniqueIndexInfo();
        return buildAllFieldValuePairArray(recordIndexInfoList);
    }

    @Override
    public Pair<RecordField, Value>[] getForeignKeyValues() {
        List<ForeignKeyIndexInfo> recordIndexInfoList = recordSchema.getForeignIndexInfo();
        return buildAllFieldValuePairArray(recordIndexInfoList);
    }

    @Override
    public long size() {
        return size;
    }

    public String toString() {

        StringBuilder sb = new StringBuilder();

        sb.append("[");

        recordSchema.getFields().forEach(recordField ->
             { sb.append("Field ")
                 .append("[")
                 .append(recordField.getFieldName())
                 .append("]")
                 .append(" ")
                 .append("[")
                 .append(getValue(recordField))
                 .append("]")
                 .append("\n");});

        sb.append("]");

        return sb.toString();
    }
}
