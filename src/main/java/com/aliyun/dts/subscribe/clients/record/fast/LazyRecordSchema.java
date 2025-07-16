package com.aliyun.dts.subscribe.clients.record.fast;

import com.aliyun.dts.subscribe.clients.common.NullableOptional;
import com.aliyun.dts.subscribe.clients.formats.avro.SourceType;
import com.aliyun.dts.subscribe.clients.record.*;
import com.aliyun.dts.subscribe.clients.record.fast.util.RecordTools;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

public class LazyRecordSchema implements RecordSchema {

    private transient LazyParseRecordImpl record;

    private DatabaseInfo databaseInfo;
    private final String schemaId;
    private long schemaVersion;
    private final String databaseName;
    private final String tableName;
    private String fullQualifiedName;

    private String logicalDatabaseName;
    private String logicalTableName;

    private List<RecordField> recordFields;
    private Map<String, Integer> recordFieldIndex;
    private Map<String, Integer> ignoreCaseFieldIndex;
    protected RecordIndexInfo primaryIndexInfo;
    protected List<RecordIndexInfo> uniqueIndexInfo;
    private String condition;
    private byte[] rawData;
    private boolean isSqlServer;
    private volatile boolean initialized = false;
    private ReentrantLock schemaLock = new ReentrantLock();

    private short crc16Id;
    public LazyRecordSchema(LazyParseRecordImpl record, String schemaId, String databaseName, String tableName) {
        this.record = record;
        this.schemaId = schemaId;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.schemaVersion = RecordTools.getMetaVersion(record);

        if (!StringUtils.isEmpty(databaseName) && !StringUtils.isEmpty(tableName)) {
            this.fullQualifiedName = databaseName + "." + tableName;
            this.crc16Id = crc16NameId(this.fullQualifiedName);
        }

        this.uniqueIndexInfo = new ArrayList<>(1);
    }

    public LazyRecordSchema(LazyParseRecordImpl record, String schemaId, String databaseName, String schemaName, String tableName, Pair<Integer, String> dbInfoPair,
                            long schemaVersion) {

        this.isSqlServer = (dbInfoPair.getKey() == SourceType.SQLServer.ordinal());

        this.record = record;
        this.schemaId = schemaId;
        this.databaseName = isSqlServer ? String.format("[%s]", databaseName) : databaseName;
        this.tableName = isSqlServer ? String.format("[%s].[%s]", schemaName, tableName) : tableName;
        this.schemaVersion = schemaVersion;

        if (!StringUtils.isEmpty(this.databaseName) && !StringUtils.isEmpty(this.tableName)) {
            this.fullQualifiedName = this.databaseName + "." + this.tableName;
            this.crc16Id = crc16NameId(this.fullQualifiedName);
        }

        this.uniqueIndexInfo = new ArrayList<>(1);
    }

    short crc16NameId(String schemaId) {
        if (null == schemaId) {
            return 0;
        }
        CRC32 crc32 = new CRC32();
        crc32.update(schemaId.getBytes(StandardCharsets.UTF_8));
        return (short) (crc32.getValue() & 0xFFFF);
    }

    public void setDatabaseInfo(DatabaseInfo databaseInfo) {
        this.databaseInfo = databaseInfo;
    }

    public void formatFieldName() {
        if (isSqlServer) {
            String left = "[";
            String right = "]";

            if (null != this.recordFields) {
                this.recordFields.forEach(field -> {
                    if (recordFieldIndex.containsKey(field.getFieldName())) {
                        recordFieldIndex.put(String.format("[%s]", field.getFieldName()), recordFieldIndex.remove(field.getFieldName()));
                    }

                    if (ignoreCaseFieldIndex.containsKey(field.getFieldName())) {
                        ignoreCaseFieldIndex.put(String.format("[%s]", field.getFieldName()), ignoreCaseFieldIndex.remove(field.getFieldName()));
                    }

                    if (!StringUtils.startsWith(field.getFieldName(), left)) {
                        ((SimplifiedRecordField) field).setFieldName(left + field.getFieldName() + right);
                    }
                });
            }
        }
    }

    protected void initPayloadIfNeeded() {
        if (initialized) {
            return;
        } else {
            schemaLock.lock();
            try {
                if (!initialized) {
                    // following call will fill record schema detail info
                    record.initPayloadIfNeeded();
                    this.initialized = true;
                    this.record = null;
                }
            } finally {
                schemaLock.unlock();
            }
        }
    }

    protected void initPayloadDone() {
        initialized = true;
    }

    @Override
    public DatabaseInfo getDatabaseInfo() {
        return this.databaseInfo;
    }

    public void setRecordFields(List<RecordField> recordFields) {
        if (null != recordFields && !recordFields.isEmpty()) {

            this.recordFields = new ArrayList<>(recordFields.size());
            this.recordFieldIndex = new HashMap<>();
            this.ignoreCaseFieldIndex = new HashMap<>();

            for (RecordField field : recordFields) {
                this.recordFields.add(field);
                this.recordFieldIndex.put(field.getFieldName(), field.getFieldPosition());
            }
        }
    }

    @Override
    public List<RecordField> getFields() {
        initPayloadIfNeeded();
        return this.recordFields;
    }

    @Override
    public int getFieldCount() {
        initPayloadIfNeeded();
        return null == this.recordFields ? -1 : this.recordFields.size();
    }

    @Override
    public RecordField getField(int index) {
        initPayloadIfNeeded();
        return null == this.recordFields ? null : this.recordFields.get(index);
    }

    @Override
    public NullableOptional<RecordField> getField(String fieldName) {
        initPayloadIfNeeded();

        if (recordFieldIndex == null) {
            return NullableOptional.empty();
        }

        Integer index = this.recordFieldIndex.get(fieldName);
        if (null == index) {
            return NullableOptional.empty();
        }
        return NullableOptional.of(this.recordFields.get(index));
    }

    @Override
    public void ignoreField(RecordField field) {
        throw new RuntimeException("capture-dstore" +
                "Un support function ignoreField(RecordField field)");
    }

    @Override
    public List<Integer> getRawDataTypes() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getFieldNames() {
        initPayloadIfNeeded();
        return recordFields == null ? null : recordFields.stream()
            .map(RecordField::getFieldName)
            .collect(Collectors.toList());
    }

    @Override
    public NullableOptional<Integer> getRawDataType(String fieldName) {
        return null;
    }

    @Override
    public NullableOptional<String> getFullQualifiedName() {
        return StringUtils.isEmpty(fullQualifiedName) ? NullableOptional.empty() : NullableOptional.of(fullQualifiedName);
    }

    @Override
    public NullableOptional<String> getDatabaseName() {
        if (null == logicalDatabaseName) {
            return StringUtils.isEmpty(this.databaseName) ? NullableOptional.empty() : NullableOptional.of(this.databaseName);
        }
        return NullableOptional.of(logicalDatabaseName);
    }

    @Override
    public NullableOptional<String> getSchemaName() {
        return getDatabaseName();
    }

    @Override
    public NullableOptional<String> getTableName() {
        if (null == logicalTableName) {
            return StringUtils.isEmpty(tableName) ? NullableOptional.empty() : NullableOptional.of(tableName);
        }
        return NullableOptional.of(logicalTableName);
    }

    @Override
    public String getSchemaIdentifier() {
        return schemaId;
    }

    public Long getSchemaVersion() {
        return schemaVersion;
    }

    public void setPrimaryIndexInfo(RecordIndexInfo indexInfo) {
        this.primaryIndexInfo = indexInfo;
    }

    @Override
    public RecordIndexInfo getPrimaryIndexInfo() {
        initPayloadIfNeeded();
        return this.primaryIndexInfo;
    }

    @Override
    public List<ForeignKeyIndexInfo> getForeignIndexInfo() {
        return Collections.emptyList();
    }

    @Override
    public void addUniqueIndexInfo(RecordIndexInfo indexInfo) {
        this.uniqueIndexInfo.add(indexInfo);
    }

    @Override
    public List<RecordIndexInfo> getUniqueIndexInfo() {
        initPayloadIfNeeded();
        return this.uniqueIndexInfo;
    }

    @Override
    public List<RecordIndexInfo> getNormalIndexInfo() {
        return Collections.emptyList();
    }

    @Override
    public String getFilterCondition() {
        return this.condition;
    }

    @Override
    public void initFilterCondition(String condition) {

    }

    @Override
    public String getCharset() {
        return "utf8";
    }

    void setLogicalDatabaseName(String name) {
        this.logicalDatabaseName = name;
    }

    void setLogicalTableName(String name) {
        this.logicalTableName = name;
    }

    public byte[] getRawData() {
        return rawData;
    }

    public void setRawData(byte[] rawData) {
        this.rawData = rawData;
    }

    public boolean isInited() {
        return initialized;
    }

    @Override
    public String toString() {
        return getDatabaseName().get() + "." + getTableName().get();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof LazyRecordSchema) {
            LazyRecordSchema other = (LazyRecordSchema) obj;
            return toString().equals(other.toString());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
