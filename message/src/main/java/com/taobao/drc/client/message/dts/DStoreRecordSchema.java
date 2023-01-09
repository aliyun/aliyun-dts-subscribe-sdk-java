package com.taobao.drc.client.message.dts;

import com.taobao.drc.client.message.dts.record.RawDataType;
import com.taobao.drc.client.message.dts.record.RecordField;
import com.taobao.drc.client.message.dts.record.RecordIndexInfo;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class DStoreRecordSchema {
    private DStoreAvroRecord record;
    private final String schemaId;
    private final String databaseName;
    private final String tableName;
    private String fullQualifiedName;

    private List<RecordField> recordFields;
    private Map<String, Integer> recordFieldIndex;
    protected RecordIndexInfo primaryIndexInfo;
    protected List<RecordIndexInfo> uniqueIndexInfo;
    private String condition;

    public DStoreRecordSchema(DStoreAvroRecord record, String schemaId, String databaseName, String tableName) {
        this.record = record;
        this.schemaId = schemaId;
        this.databaseName = databaseName;
        this.tableName = tableName;
        if (!StringUtils.isEmpty(databaseName) && !StringUtils.isEmpty(tableName)) {
            this.fullQualifiedName = databaseName + "." + tableName;
        }

        this.uniqueIndexInfo = new ArrayList<RecordIndexInfo>(1);
    }

    public void setRecordFields(List<RecordField> recordFields) {
        if (null != recordFields && !recordFields.isEmpty()) {

            this.recordFields = new ArrayList<RecordField>(recordFields.size());
            this.recordFieldIndex = new HashMap<String, Integer>();

            for (RecordField field : recordFields) {
                this.recordFields.add(field);
                this.recordFieldIndex.put(field.getFieldName(), field.getFieldPosition());
            }
        }
    }

    public List<RecordField> getFields() {
        record.initBody();
        return this.recordFields;
    }

    public int getFieldCount() {
        return getFieldCount(true);
    }

    public int getFieldCount(boolean load) {
        if (load) {
            record.initBody();
        }
        return null == this.recordFields ? -1 : this.recordFields.size();
    }

    public RecordField getField(int index) {
        record.initBody();
        return this.recordFields.get(index);
    }

    public RecordField getField(String fieldName) {
        record.initBody();
        return this.getField(fieldName, true);
    }

    public RecordField getField(String fieldName, boolean load) {
        if (load) {
            record.initBody();
        }
        int index = this.recordFieldIndex.get(fieldName);
        return this.recordFields.get(index);
    }

    public List<RawDataType> getRawDataTypes() {
        record.initBody();
        List<RawDataType> rawDataTypes = new ArrayList<RawDataType>(recordFields.size());
        for (RecordField f : recordFields) {
            rawDataTypes.add(f.getRawDataType());
        }
        return rawDataTypes;
    }

    public List<String> getFieldNames() {
        record.initBody();
        List<String> fieldNames = new ArrayList<String>(recordFields.size());
        for (RecordField f : recordFields) {
            fieldNames.add(f.getFieldName());
        }
        return fieldNames;
    }

    public RawDataType getRawDataType(String fieldName) {
        record.initBody();
        RecordField recordField = getField(fieldName);
        if (null == recordField) {
            return null;
        } else {
            return recordField.getRawDataType();
        }
    }

    public String getFullQualifiedName() {
        return StringUtils.trimToEmpty(fullQualifiedName);
    }

    public String getDatabaseName() {
        return StringUtils.trimToEmpty(this.databaseName);
    }

    public String getSchemaName() {
        return getDatabaseName();
    }

    public String getTableName() {
        return StringUtils.trimToEmpty(this.tableName);
    }

    public String getSchemaIdentifier() {
        return schemaId;
    }

    public void setPrimaryIndexInfo(RecordIndexInfo indexInfo) {
        this.primaryIndexInfo = indexInfo;
    }

    public RecordIndexInfo getPrimaryIndexInfo() {
        record.initBody();
        return this.primaryIndexInfo;
    }

    public List<RecordIndexInfo> getForeignIndexInfo() {
        return Collections.emptyList();
    }

    public void addUniqueIndexInfo(RecordIndexInfo indexInfo) {
        this.uniqueIndexInfo.add(indexInfo);
    }

    public List<RecordIndexInfo> getUniqueIndexInfo() {
        record.initBody();
        return this.uniqueIndexInfo;
    }

    public List<RecordIndexInfo> getNormalIndexInfo() {
        return Collections.emptyList();
    }

    public void rearrangeRecordFields() {}

    public String getFilterCondition() {
        return this.condition;
    }

    public void initFilterCondition(String condition) {
        this.condition = condition;
    }
}
