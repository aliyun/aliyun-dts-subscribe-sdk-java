package com.taobao.drc.client.message.dts;

import com.taobao.drc.client.message.dts.record.RecordField;
import com.taobao.drc.client.message.dts.record.RecordIndexInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

public class DStoreRowImage {

    private final DStoreRecordSchema recordSchema;
    private final DStoreRawValue[] values;
    private long size;

    public DStoreRowImage(DStoreRecordSchema recordSchema) {
        this(recordSchema, recordSchema.getFieldCount());
    }

    public DStoreRowImage(DStoreRecordSchema recordSchema, int fieldCount) {
        this.recordSchema = recordSchema;
        this.values = new DStoreRawValue[fieldCount];
    }

    public DStoreRawValue[] getValues() {
        return this.values;
    }

    public DStoreRawValue getValue(int index) {
        return values[index];
    }

    public DStoreRawValue getValue(RecordField field) {
        return getValue(field.getFieldPosition());
    }

    public DStoreRawValue getValue(String fieldName) {
        RecordField recordField = recordSchema.getField(fieldName);
        if (null == recordField) {
            return null;
        }
        return getValue(recordField);
    }

    private void accumulateSize(DStoreRawValue value) {
        if (null != value) {
            size += value.size();
        }
    }

    public void setValue(int i, DStoreRawValue value) {
        values[i] = value;

        accumulateSize(value);
    }

    public void setValue(String fieldName, DStoreRawValue value) {
        RecordField recordField = recordSchema.getField(fieldName);
        setValue(recordField, value);
    }

    public void setValue(RecordField field, DStoreRawValue value) {
        int index = field.getFieldPosition();
        setValue(index, value);
    }

    public Map<String, DStoreRawValue> toMap() {
        Map<String, DStoreRawValue> valueMap = new TreeMap<String, DStoreRawValue>();
        int i = 0;
        for (RecordField field : recordSchema.getFields()) {
            valueMap.put(StringUtils.trimToEmpty(field.getFieldName()).toUpperCase(), values[i]);
            i++;
        }
        return valueMap;
    }

    public Pair<RecordField, DStoreRawValue>[] buildFieldValuePairArray(Collection<RecordField> recordFields) {
        Pair<RecordField, DStoreRawValue>[] rs = new ImmutablePair[recordFields.size()];
        int index = 0;
        for (RecordField recordField : recordFields) {
            rs[index] = Pair.of(recordField, getValue(recordField));
            index++;
        }

        return rs;
    }

    public Pair<RecordField, DStoreRawValue>[] getPrimaryKeyValues() {
        RecordIndexInfo recordIndexInfo = recordSchema.getPrimaryIndexInfo();
        if (null == recordIndexInfo) {
            return null;
        }

        return buildFieldValuePairArray(recordIndexInfo.getIndexFields());
    }

    private Pair<RecordField, DStoreRawValue>[] buildAllFieldValuePairArray(List<RecordIndexInfo> recordIndexInfoList) {
        if (null == recordIndexInfoList || recordIndexInfoList.isEmpty()) {
            return null;
        }
        Set<RecordField> recordFieldSet = new TreeSet<RecordField>();
        for (RecordIndexInfo indexInfo : recordIndexInfoList) {
            for (RecordField f : indexInfo.getIndexFields()) {
                recordFieldSet.add(f);
            }
        }
        return buildFieldValuePairArray(recordFieldSet);
    }

    public Pair<RecordField, DStoreRawValue>[] getUniqueKeyValues() {
        List<RecordIndexInfo> recordIndexInfoList = recordSchema.getUniqueIndexInfo();
        return buildAllFieldValuePairArray(recordIndexInfoList);
    }

    public Pair<RecordField, DStoreRawValue>[] getForeignKeyValues() {
        List<RecordIndexInfo> recordIndexInfoList = recordSchema.getForeignIndexInfo();
        return buildAllFieldValuePairArray(recordIndexInfoList);
    }

    public long size() {
        return size;
    }

    @Override
    public String toString() {
        return StringUtils.join(values, ",");
    }
}
