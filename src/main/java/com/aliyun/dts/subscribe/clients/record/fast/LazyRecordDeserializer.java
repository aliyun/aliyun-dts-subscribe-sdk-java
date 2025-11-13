package com.aliyun.dts.subscribe.clients.record.fast;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.dts.subscribe.clients.formats.avro.SourceType;
import com.aliyun.dts.subscribe.clients.formats.util.ObjectNameUtils;
import com.aliyun.dts.subscribe.clients.record.*;
import com.aliyun.dts.subscribe.clients.record.impl.DefaultRowImage;
import com.aliyun.dts.subscribe.clients.record.value.*;
import org.apache.avro.io.Decoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LazyRecordDeserializer {

    private static final Logger LOG = LoggerFactory.getLogger(LazyRecordDeserializer.class);
    private static final int POSTGRESQL_BOOL_TYPE = 16;
    private static final int POSTGRESQL_JSONB_TYPE = 3802;
    static OperationType[] operationDeserializers = new OperationType[OperationType.values().length];

    static {
        /**
         * code: 0
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.INSERT
         */
        operationDeserializers[0] = OperationType.INSERT;

        /**
         * code: 1
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.UPDATE
         */
        operationDeserializers[1] = OperationType.UPDATE;

        /**
         * code: 2
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.DELETE
         */
        operationDeserializers[2] = OperationType.DELETE;

        /**
         * code: 3
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.DDL
         */
        operationDeserializers[3] = OperationType.DDL;

        /**
         * code: 4
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.BEGIN
         */
        operationDeserializers[4] = OperationType.BEGIN;

        /**
         * code: 5
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.COMMIT
         */
        operationDeserializers[5] = OperationType.COMMIT;

        /**
         * code: 6
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.ROLLBACK
         */
        operationDeserializers[6] = OperationType.ROLLBACK;

        /**
         * code: 7
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.ABORT
         */
        operationDeserializers[7] = OperationType.ABORT;

        /**
         * code: 8
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.HEARTBEAT
         */
        operationDeserializers[8] = OperationType.HEARTBEAT;

        /**
         * code: 9
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.CHECKPOINT
         */
        operationDeserializers[9] = OperationType.CHECKPOINT;

        /**
         * code: 10
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.COMMAND
         * but treat mongodb command as ddl
         */
        operationDeserializers[10] = OperationType.DDL;

        /**
         * code: 11
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.FILL
         */
        operationDeserializers[11] = OperationType.FILL;

        /**
         * code: 12
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.FINISH
         */
        operationDeserializers[12] = OperationType.FINISH;

        /**
         * code: 13
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.CONTROL
         */
        operationDeserializers[13] = OperationType.CONTROL;

        /**
         * code: 14
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.RDB
         */
        operationDeserializers[14] = OperationType.RDB;

        /**
         * code: 15
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.NOOP
         */
        operationDeserializers[15] = OperationType.NOOP;

        /**
         * code: 16
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.INIT
         */
        operationDeserializers[16] = OperationType.INIT;
    }

    protected static DateTime deserializeDateTime(Decoder decoder, int sourceTypeCode)
            throws IOException {

        DateTime dateTime = new DateTime();
        int unionIndex = decoder.readIndex();
        if (1 == unionIndex) {
            dateTime.setSegments(DateTime.SEG_YEAR);
            dateTime.setYear(decoder.readInt());
        } else {
            decoder.readNull();
        }

        unionIndex = decoder.readIndex();
        if (1 == unionIndex) {
            dateTime.setSegments(DateTime.SEG_MONTH);
            dateTime.setMonth(decoder.readInt());
        } else {
            decoder.readNull();
        }

        unionIndex = decoder.readIndex();
        if (1 == unionIndex) {
            dateTime.setSegments(DateTime.SEG_DAY);
            dateTime.setDay(decoder.readInt());
        } else {
            decoder.readNull();
        }

        unionIndex = decoder.readIndex();
        if (1 == unionIndex) {
            dateTime.setSegments(DateTime.SEG_HOUR);
            dateTime.setHour(decoder.readInt());
        } else {
            decoder.readNull();
        }

        unionIndex = decoder.readIndex();
        if (1 == unionIndex) {
            dateTime.setSegments(DateTime.SEG_MINITE);
            dateTime.setMinute(decoder.readInt());
        } else {
            decoder.readNull();
        }

        unionIndex = decoder.readIndex();
        if (1 == unionIndex) {
            dateTime.setSegments(DateTime.SEG_SECOND);
            dateTime.setSecond(decoder.readInt());
        } else {
            decoder.readNull();
        }

        unionIndex = decoder.readIndex();
        if (1 == unionIndex) {
            dateTime.setSegments(DateTime.SEG_NAONS);
            int naons = decoder.readInt();
            if (sourceTypeCode == SourceType.PostgreSQL.ordinal()
                    || sourceTypeCode == SourceType.MySQL.ordinal()) {
                naons *= 1000;
            }
            dateTime.setNaons(naons);
        } else {
            decoder.readNull();
        }
        return dateTime;
    }

    interface ValueDeserializer<T> {
        T deserialize(BufferWarpBinaryDecoder decoder, int sourceCode, RecordField field) throws IOException;
    }

    static ValueDeserializer<Value>[] valueDeserializers = new ValueDeserializer[13];
    static final int MAX_LONG_VALUE_LENGTH = String.valueOf(Long.MAX_VALUE).length() - 1;
    static Map<String, ObjectType> objectTypes = new ConcurrentHashMap<>((int) (ObjectType.values().length * 1.34));

    static ObjectType getObjectTypeByTypename(String typename) {
        ObjectType objectType = objectTypes.get(typename);
        if (null == objectType) {
            objectType = ObjectType.parse(typename);
            objectTypes.putIfAbsent(typename, objectType);
        }
        return objectType;
    }

    static {

        /**
         * code: 0
         * name: null
         */
        valueDeserializers[0] = (decoder, sourceTypeCode, field) -> {
            decoder.readNull();
            return null;
        };

        /**
         * code: 1
         * name: com.alibaba.amp.any.common.record.formats.avro.Integer
         */
        valueDeserializers[1] = (decoder, sourceTypeCode, field) -> {
            decoder.readInt();
            String text = decoder.readAsciiString();
            if (text.length() < MAX_LONG_VALUE_LENGTH) {
                return new IntegerNumeric(Long.parseLong(text));
            } else {
                return new IntegerNumeric(text);
            }
        };

        /**
         * code: 2
         * name: com.alibaba.amp.any.common.record.formats.avro.Character
         */
        valueDeserializers[2] = (decoder, sourceTypeCode, field) -> {
            String charset = decoder.readString();
            ByteBuffer data = decoder.readBytes(null);
            return new StringValue(data, charset);
        };

        /**
         * code: 3
         * name: com.alibaba.amp.any.common.record.formats.avro.Decimal
         */
        valueDeserializers[3] = (decoder, sourceTypeCode, field) -> {
            String text = decoder.readAsciiString();
            decoder.readInt();
            decoder.readInt();
            try {
                DecimalNumeric numeric = new DecimalNumeric(text);
                return numeric;
            } catch (NumberFormatException ex) {
                return new SpecialNumeric(text);
            }
        };

        /**
         * code: 4
         * name: com.alibaba.amp.any.common.record.formats.avro.Float
         */
        valueDeserializers[4] = (decoder, sourceTypeCode, field) -> {
            FloatNumeric numeric = new FloatNumeric(decoder.readDouble());
            decoder.readInt();
            decoder.readInt();
            return numeric;
        };

        /**
         * code: 5
         * name: com.alibaba.amp.any.common.record.formats.avro.Timestamp
         */
        valueDeserializers[5] = (decoder, sourceTypeCode, field) -> new UnixTimestamp(decoder.readLong(), decoder.readInt());

        /**
         * code: 6
         * name: com.alibaba.amp.any.common.record.formats.avro.DateTime
         */
        valueDeserializers[6] = (decoder, sourceTypeCode, field) -> deserializeDateTime(decoder, sourceTypeCode);

        /**
         * code: 7
         * name: com.alibaba.amp.any.common.record.formats.avro.TimestampWithTimeZone
         */
        valueDeserializers[7] = (decoder, sourceTypeCode, field) -> {
            DateTime dateTime = deserializeDateTime(decoder, sourceTypeCode);

            String timeZone = decoder.readString();
            dateTime.setSegments(DateTime.SEG_TIMEZONE);
            dateTime.setTimeZone(timeZone);
            return dateTime;
        };

        /**
         * code: 8
         * name: com.alibaba.amp.any.common.record.formats.avro.BinaryGeometry
         */
        valueDeserializers[8] = (decoder, sourceTypeCode, field) -> {
            // skip type string
            decoder.skipString();
            return new WKBGeometry(decoder.readBytes(null));
        };

        /**
         * code: 9
         * name: com.alibaba.amp.any.common.record.formats.avro.TextGeometry
         */
        valueDeserializers[9] = (decoder, sourceTypeCode, field) -> {
            // skip type string
            decoder.skipString();
            return new WKTGeometry(decoder.readString());
        };

        /**
         * code: 10
         * name: com.alibaba.amp.any.common.record.formats.avro.BinaryObject
         */
        valueDeserializers[10] = (decoder, sourceTypeCode, field) -> {
            BinaryEncodingObject binaryEncodingObject = new BinaryEncodingObject(getObjectTypeByTypename(decoder.readString()), decoder.readBytes(null));
            if (SourceType.PostgreSQL.ordinal() == sourceTypeCode
                && field != null && POSTGRESQL_JSONB_TYPE == field.getRawDataType().getTypeId())  {
                return null == binaryEncodingObject.getData() ? new StringValue(null, null) : new StringValue(new String(binaryEncodingObject.getData().array()));
            } else {
                return binaryEncodingObject;
            }
        };

        /**
         * code: 11
         * name: com.alibaba.amp.any.common.record.formats.avro.TextObject
         */
        valueDeserializers[11] = (decoder, sourceTypeCode, field) -> {
            TextEncodingObject textObject = new TextEncodingObject(getObjectTypeByTypename(decoder.readString()), decoder.readString());
            if (SourceType.PostgreSQL.ordinal() == sourceTypeCode) {
                if (field != null && POSTGRESQL_BOOL_TYPE == field.getRawDataType().getTypeId()) {
                    if (textObject.toString().equalsIgnoreCase("t") || textObject.toString().equalsIgnoreCase("true")) {
                        return new IntegerNumeric(1);
                    } else {
                        return new IntegerNumeric(0);
                    }
                }
            }
            return textObject;
        };

        /**
         * code: 12
         * name: com.alibaba.amp.any.common.record.formats.avro.EmptyObject
         */
        valueDeserializers[12] = (decoder, sourceTypeCode, field) -> {
            decoder.readEnum();
            return new NoneValue();
        };
    }

    private boolean schemaCacheEnable;
    private Map<String, LazyRecordSchema> cachedSchema;

    public LazyRecordDeserializer(boolean schemaCacheEnable) {
        this.schemaCacheEnable = schemaCacheEnable;
        this.cachedSchema = new HashMap<>();
    }

    public void deserializeHeader(BufferWarpBinaryDecoder decoder, LazyParseRecordImpl record) throws IOException {

        // skip version
        decoder.readInt();
        record.setId(decoder.readLong());
        record.setTimestamp(decoder.readLong());
        String sourcePosition = decoder.readAsciiString();
        record.setSourcePosition(sourcePosition);
        record.setSourceSafePosition(decoder.readString());
        record.setTransactionId(decoder.readAsciiString());

        Pair<Integer, String> dbInfoPair = deserializeSource(decoder);
        if ((SourceType.MySQL.ordinal() == dbInfoPair.getKey()
                || SourceType.PostgreSQL.ordinal() == dbInfoPair.getKey()) && sourcePosition.indexOf("<->") >= 0) {
            //PG物理日志reader sourcePosition一定包含<->,并且Datetime毫秒位使用的是那秒
            record.setSourceTypeCode(SourceType.PPAS.ordinal());
        } else {
            record.setSourceTypeCode(dbInfoPair.getKey());
        }
        DatabaseInfo databaseInfo = new DatabaseInfo(SourceType.values()[dbInfoPair.getKey()].name(), dbInfoPair.getValue());
        record.setSourceTypeAndVersion(Pair.of(databaseInfo.getDatabaseType(), databaseInfo.getVersion()));
        record.setOperationType(operationDeserializers[decoder.readEnum()]);

        // deserialize object names
        LazyRecordSchema recordSchema = null;
        String objectName = null;
        int unionIndex = decoder.readIndex();
        if (0 == unionIndex) {
            decoder.readNull();
        } else if (1 == unionIndex) {
            // parse db/schema/tb name
            objectName = decoder.readString();
        }

        //skip Long list
        deserializeLongList(decoder, true);

        // deserialize tags
        Map<String, String> tags = deserializeMap(decoder);
        record.setExtendedProperty(tags);
        if (null != objectName) {
            long metaVersion = 0;
            String tagVersion = tags.get("metaVersion");
            if (null != tagVersion) {
                try {
                    metaVersion = Long.parseLong(tagVersion);
                } catch (Throwable ignored) {
                }
            }
            if (this.schemaCacheEnable) {
                recordSchema = this.cachedSchema.get(objectName);
                if (null != recordSchema && (metaVersion > 0 && metaVersion != recordSchema.getSchemaVersion())) {
                    recordSchema = null;
                }
            }
            if (null == recordSchema) {
                Triple<String, String, String> nameTriple = deserializeNameTriple(objectName);
                recordSchema = new LazyRecordSchema(record, nameTriple.toString(), nameTriple.getLeft(), nameTriple.getMiddle(), nameTriple.getRight(), dbInfoPair,
                        metaVersion);
                if (this.schemaCacheEnable) {
                    LazyRecordSchema oldRecordSchema = this.cachedSchema.put(objectName, recordSchema);
                    LOG.info("Put new schema:" + objectName + ", metaVersion: " + metaVersion
                            + (null == oldRecordSchema ? "" : " oldMetaVersion:" + oldRecordSchema.getSchemaVersion()));
                }
            }
            // try extract logical names
            recordSchema.setLogicalDatabaseName(tags.get("l_db_name"));
            recordSchema.setLogicalTableName(tags.get("l_tb_name"));
            record.setRecordSchema(recordSchema);
        }
    }

    public void deserializePayload(BufferWarpBinaryDecoder decoder, LazyParseRecordImpl record) throws IOException {

        // deserialize pk/uk info from tag field
        deserializeFieldListAndIndex(decoder, record, record.getExtendedProperty().get("pk_uk_info"));
        if (null != record.getSchema(false)) {
            record.getSchema(false).initPayloadDone();
        }

        record.setBeforeImage(deserializeRowImage(decoder, record.getSchema(false), record.getSourceTypeCode()));
        record.setAfterImage(deserializeRowImage(decoder, record.getSchema(false), record.getSourceTypeCode()));

        if (!decoder.isEnd()) {
            record.setBornTimestamp(decoder.readLong());
        }
    }

    public void flushSchemaCache() {
        this.cachedSchema.clear();
    }

    protected static Pair<Integer, String> deserializeSource(BufferWarpBinaryDecoder decoder)
            throws IOException {
        return Pair.of(decoder.readEnum(), decoder.readAsciiString());
    }

    protected static Triple<String, String, String> deserializeNameTriple(String mixedNames) {
        String dbName = null;
        String schemaName = null;
        String tableName = null;

        String[] dbPair = ObjectNameUtils.uncompressionObjectName(mixedNames);
        if (dbPair != null) {
            if (dbPair.length < 1 || dbPair.length > 3) {
                throw new RuntimeException("capture-dstore" +
                    "Invalid db table name pair for mixed [" + mixedNames + "]");
            } else {
                if (dbPair.length > 1) {
                    schemaName = dbPair[dbPair.length - 2];
                    tableName = dbPair[dbPair.length - 1];
                }
                dbName = dbPair[0];
            }
        }
        return Triple.of(dbName, schemaName, tableName);
    }

    protected static List<Long> deserializeLongList(Decoder decoder, boolean nullable) throws IOException {
        int longIndex = decoder.readIndex();
        if (1 == longIndex) {
            long chunkLen = (decoder.readArrayStart());
            List<Long> longList = new ArrayList<Long>();
            if (chunkLen > 0) {
                do {
                    for (int counter = 0; (counter < chunkLen); ++counter) {
                        longList.add(decoder.readLong());
                    }
                    chunkLen = decoder.arrayNext();
                } while (chunkLen > 0);
            }
            return longList;
        } else {
            decoder.readNull();
            return nullable ? null : new ArrayList<Long>(1);
        }
    }

    protected static Map<String, String> deserializeMap(Decoder decoder) throws IOException {
        Map<String, String> map = new LinkedHashMap<>();
        long chunkLen = (decoder.readArrayStart());
        if (chunkLen > 0) {
            do {
                for (int counter = 0; (counter < chunkLen); ++counter) {
                    String key = decoder.readString();
                    String value = decoder.readString();
                    map.put(key, value);
                }
                chunkLen = decoder.mapNext();
            } while (chunkLen > 0);
        }
        return map;
    }

    protected static Pair<Map<String, Boolean>, Map<String, Pair<Boolean, List<String>>>> deserializePkUkInfo(String text) throws IOException {
        if (StringUtils.isEmpty(text)) {
            return Pair.of(Collections.emptyMap(), Collections.emptyMap());
        }

        Map<String, Pair<Boolean, List<String>>> pkUkInfo = new LinkedHashMap<>();
        Map<String, Boolean> columnUkInfo = new HashMap<>();
        JSONObject jsonObject = JSON.parseObject(text);
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            String key = entry.getKey();
            JSONArray jsonArray = (JSONArray) entry.getValue();
            boolean isPrimaryKey = StringUtils.equals("PRIMARY", key);
            List<String> columns = new ArrayList<>();
            for (Object value : jsonArray) {
                String colName = (String) value;
                columns.add(colName);
                if (isPrimaryKey || (!columnUkInfo.containsKey(colName))) {
                    columnUkInfo.put(colName, isPrimaryKey);
                }
            }
            pkUkInfo.put(key, Pair.of(isPrimaryKey, columns));
        }
        return Pair.of(columnUkInfo, pkUkInfo);
    }

    protected static void skipFixedAndCheck(BufferWarpBinaryDecoder decoder, byte[] fixed) throws IOException {

//        byte[] source = decoder.getBuf();
//        int sourcePos = decoder.getPos();
//        int ret = 0;
//        for (int i = 0; (0 == ret) && i < fixed.length; ++i) {
//            ret = source[sourcePos + i] - fixed[i++];
//        }
//        if (0 != ret) {
//            throw new IOException("Schema check error.");
//        }
        decoder.skipFixed(fixed.length);
    }

    protected static void deserializeFieldListAndIndex(BufferWarpBinaryDecoder decoder,
                                                       LazyParseRecordImpl record,
                                                       String pkukInfo) throws IOException {
        LazyRecordSchema recordSchema = record.getSchema(false);
        int fieldIndex = decoder.readIndex();
        if (2 == fieldIndex) {
            if (recordSchema.isInited()) {
                skipFixedAndCheck(decoder, recordSchema.getRawData());
            } else {
                // parse pk/uk names
                Pair<Map<String, Boolean>, Map<String, Pair<Boolean, List<String>>>> pkUkInfo
                        = deserializePkUkInfo(pkukInfo);

                Pair<List<RecordField>, byte[]> recordFieldAndLength = deserializeFieldList(decoder, pkUkInfo.getKey());
                recordSchema.setRecordFields(recordFieldAndLength.getLeft());
                recordSchema.setRawData(recordFieldAndLength.getRight());
                Map<String, RecordField> recordFields =
                        recordFieldAndLength.getLeft().stream().collect(Collectors.toMap(RecordField::getFieldName,
                        Function.identity(), (oldValue, newValue) -> newValue));
                // build uk index infos
                Map<String, Pair<Boolean, List<String>>> pkUkInfoMap = pkUkInfo.getRight();
                for (Map.Entry<String, Pair<Boolean, List<String>>> entry : pkUkInfoMap.entrySet()) {

                    boolean isPrimaryKey = entry.getValue().getLeft();
                    RecordIndexInfo recordIndexInfo = new RecordIndexInfo(
                            isPrimaryKey ? RecordIndexInfo.IndexType.PrimaryKey : RecordIndexInfo.IndexType.UniqueKey);
                    for (String ukFieldName : entry.getValue().getRight()) {
                        if (!recordFields.containsKey(ukFieldName) || null == recordFields.get(ukFieldName)) {
                            throw new RuntimeException(ukFieldName + " not found in record [" + recordSchema.getFullQualifiedName() + "]");
                        } else {
                            recordIndexInfo.addField(recordFields.get(ukFieldName));
                        }
                    }
                    if (isPrimaryKey) {
                        recordSchema.setPrimaryIndexInfo(recordIndexInfo);
                    } else {
                        recordSchema.addUniqueIndexInfo(recordIndexInfo);
                    }
                }
                // format field name, same as full migration.
                recordSchema.formatFieldName();
            }
        } else if (1 == fieldIndex) {
            decoder.readString();
        } else {
            decoder.readNull();
            LazyRecordSchema ddlRecordSchema = record.getSchema(false);
            if (OperationType.DDL == record.getOperationType()) {
                if (null == ddlRecordSchema) {
                    ddlRecordSchema = new LazyRecordSchema(record, null, null, null);
                    record.setRecordSchema(ddlRecordSchema);
                }
                RecordField ddlField = new SimplifiedRecordField("ddl", 0);
                RecordField dataField = new SimplifiedRecordField("data", 0);
                ddlRecordSchema.setRecordFields(Arrays.asList(ddlField, dataField));
            }
        }
        // add field info for mongodb source for etl add column
        if (record.getSourceTypeCode() == SourceType.MongoDB.ordinal()) {
            if (record.getOperationType() == OperationType.INSERT || record.getOperationType() == OperationType.UPDATE || record.getOperationType() == OperationType.DELETE) {
                recordSchema.setRecordFields(Arrays.asList(new SimplifiedRecordField("_value",0),
                        new SimplifiedRecordField("_id", 0)));
            }
        }
    }

    protected static Pair<List<RecordField>, byte[]> deserializeFieldList(BufferWarpBinaryDecoder decoder, Map<String, Boolean> colPkUkInfo) throws IOException {
        int metaRawPos = decoder.getPos();
        long chunkLen = (decoder.readArrayStart());
        List<RecordField> recordFields = new ArrayList<>();
        int fieldPosition = 0;
        if (chunkLen > 0) {
            do {
                for (int counter = 0; (counter < chunkLen); ++counter) {
                    String fieldName = decoder.readString();
                    int fieldTypeNumber = decoder.readInt();
                    SimplifiedRecordField recordField = new SimplifiedRecordField(fieldName, fieldTypeNumber);

                    Boolean isPkCol = colPkUkInfo.get(fieldName);
                    if (null != isPkCol) {
                        recordField.setUnique(true);
                        recordField.setPrimary(isPkCol);
                    }
                    recordField.setFieldPosition(fieldPosition);
                    recordFields.add(recordField);
                    fieldPosition++;
                }
                chunkLen = decoder.arrayNext();
            } while (chunkLen > 0);
        }
        int metaRawLen = decoder.getPos() - metaRawPos;
        byte[] rawData = new byte[metaRawLen];
        System.arraycopy(decoder.getBuf(), metaRawPos, rawData, 0, metaRawLen);
        return Pair.of(recordFields, rawData);
    }

    protected static DefaultRowImage deserializeRowImage(BufferWarpBinaryDecoder decoder, LazyRecordSchema recordSchema, int souceTypeCode) throws IOException {

        DefaultRowImage rowImage = null;
        int imageIndex = decoder.readIndex();
        if (2 == imageIndex) {
            long chunkLen = (decoder.readArrayStart());
            if (chunkLen > 0) {
                rowImage = new DefaultRowImage(recordSchema, recordSchema.getFieldCount() > 0 ? recordSchema.getFieldCount() : (int) chunkLen);
                int index = 0;
                do {
                    for (int counter = 0; (counter < chunkLen); counter++) {
                        int type = (decoder.readIndex());
                        RecordField field = null;
                        if (recordSchema.getFieldCount() > counter) {
                            field = recordSchema.getField(counter);
                        }
                        rowImage.setValue(index, valueDeserializers[type].deserialize(decoder, souceTypeCode, field));
                        ++index;
                    }
                    chunkLen = (decoder.arrayNext());
                } while (chunkLen > 0);
            }
        } else if (1 == imageIndex) {
            rowImage = new DefaultRowImage(recordSchema, recordSchema.getFieldCount());
            rowImage.setValue(0, new StringValue(decoder.readString()));
        } else {
            decoder.readNull();
            return null;
        }
        return rowImage;
    }
}
