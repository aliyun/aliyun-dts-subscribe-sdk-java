package com.taobao.drc.client.message.dts;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.message.ByteString;
import com.taobao.drc.client.message.DataMessage;
import com.taobao.drc.client.message.dts.record.OperationType;
import com.taobao.drc.client.message.dts.record.RecordField;
import com.taobao.drc.client.message.dts.record.RecordIndexInfo;
import com.taobao.drc.client.message.dts.record.formats.SourceType;
import com.taobao.drc.client.message.dts.record.impl.DefaultRawDataType;
import com.taobao.drc.client.message.dts.record.utils.BytesUtil;
import com.taobao.drc.client.message.dts.record.utils.ObjectNameUtils;
import com.taobao.drc.client.message.dts.record.value.BinaryEncodingObject;
import com.taobao.drc.client.message.dts.record.value.DateTime;
import com.taobao.drc.client.message.dts.record.value.ObjectType;
import com.taobao.drc.client.message.dts.record.value.UnixTimestamp;
import org.apache.avro.io.Decoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;


public class DStoreRecordDeserializer {
    protected static final String BINARY_ENCODE = "binary";
    protected static final int MYSQL_TYPE_TIMESTAMP = 7;
    protected static final int MYSQL_TYPE_DATE = 10;
    protected static final int MYSQL_TYPE_YEAR = 13;
    protected static final int MYSQL_TYPE_VARCHAR = 15;
    protected static final int MYSQL_TYPE_BLOB = 252;
    protected static final int MYSQL_TYPE_STRING = 254;
    protected static final int MYSQL_TYPE_253 = 253;
    protected static final int MYSQL_TYPE_UNKNOWN = 17;

    static DataMessage.Record.Type[] operationDeserializers = new DataMessage.Record.Type[OperationType.values().length];
    static DBType[] dbTypeDeserializers = new DBType[SourceType.values().length];

    static {
        /**
         * code: 0
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.INSERT
         */
        operationDeserializers[0] = DataMessage.Record.Type.INSERT;

        /**
         * code: 1
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.UPDATE
         */
        operationDeserializers[1] = DataMessage.Record.Type.UPDATE;

        /**
         * code: 2
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.DELETE
         */
        operationDeserializers[2] = DataMessage.Record.Type.DELETE;

        /**
         * code: 3
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.DDL
         */
        operationDeserializers[3] = DataMessage.Record.Type.DDL;

        /**
         * code: 4
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.BEGIN
         */
        operationDeserializers[4] = DataMessage.Record.Type.BEGIN;

        /**
         * code: 5
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.COMMIT
         */
        operationDeserializers[5] = DataMessage.Record.Type.COMMIT;

        /**
         * code: 6
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.ROLLBACK
         */
        operationDeserializers[6] = DataMessage.Record.Type.ROLLBACK;

        /**
         * code: 7
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.ABORT
         */
        operationDeserializers[7] = DataMessage.Record.Type.UNKNOWN;

        /**
         * code: 8
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.HEARTBEAT
         */
        operationDeserializers[8] = DataMessage.Record.Type.HEARTBEAT;

        /**
         * code: 9
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.CHECKPOINT
         */
        operationDeserializers[9] = DataMessage.Record.Type.UNKNOWN;

        /**
         * code: 10
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.COMMAND
         */
        operationDeserializers[10] = DataMessage.Record.Type.UNKNOWN;

        /**
         * code: 11
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.FILL
         */
        operationDeserializers[11] = DataMessage.Record.Type.UNKNOWN;

        /**
         * code: 12
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.FINISH
         */
        operationDeserializers[12] = DataMessage.Record.Type.UNKNOWN;

        /**
         * code: 13
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.CONTROL
         */
        operationDeserializers[13] = DataMessage.Record.Type.UNKNOWN;

        /**
         * code: 14
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.RDB
         */
        operationDeserializers[14] = DataMessage.Record.Type.UNKNOWN;

        /**
         * code: 15
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.NOOP
         */
        operationDeserializers[15] = DataMessage.Record.Type.UNKNOWN;

        /**
         * code: 16
         * name: com.alibaba.amp.any.common.record.formats.avro.Operation.INIT
         */
        operationDeserializers[16] = DataMessage.Record.Type.UNKNOWN;
        dbTypeDeserializers[0] = DBType.MYSQL;
        dbTypeDeserializers[1] = DBType.ORACLE;
        /**
         * SQLServer
         */
        dbTypeDeserializers[2] = DBType.UNKNOWN;
        /**
         * PostgreSQL
         */
        dbTypeDeserializers[3] = DBType.UNKNOWN;
        /**
         * MongoDB
         */
        dbTypeDeserializers[4] = DBType.UNKNOWN;
        /**
         * Redis
         */
        dbTypeDeserializers[5] = DBType.UNKNOWN;
        /**
         * DB2
         */
        dbTypeDeserializers[6] = DBType.UNKNOWN;
        /**
         * PPAS
         */
        dbTypeDeserializers[7] = DBType.UNKNOWN;
        /**
         * DRDS
         */
        dbTypeDeserializers[8] = DBType.MYSQL;
        /**
         * HBASE
         */
        dbTypeDeserializers[9] = DBType.HBASE;
        /**
         * HDFS
         */
        dbTypeDeserializers[10] = DBType.UNKNOWN;
        /**
         * FILE
         */
        dbTypeDeserializers[11] = DBType.UNKNOWN;
        /**
         * OTHER
         */
        dbTypeDeserializers[12] = DBType.UNKNOWN;
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
        T deserialize(Decoder decoder, int sourceCode) throws IOException;
    }

    static ValueDeserializer<DStoreRawValue>[] valueDeserializers = new ValueDeserializer[13];

    static {

        /**
         * code: 0
         * name: null
         */
        valueDeserializers[0] = new ValueDeserializer<DStoreRawValue>() {
            @Override
            public DStoreRawValue deserialize(Decoder decoder, int sourceTypeCode) throws IOException {
                decoder.readNull();
                return null;
            }
        };

        /**
         * code: 1
         * name: com.alibaba.amp.any.common.record.formats.avro.Integer
         */
        valueDeserializers[1] = new ValueDeserializer<DStoreRawValue>() {
            @Override
            public DStoreRawValue deserialize(Decoder decoder, int sourceTypeCode) throws IOException {
                decoder.readInt();
                byte[] data = decoder.readString().getBytes();
                //return new IntegerNumeric(decoder.readString());
                return new DStoreRawValue(new ByteString(data), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.INT64.ordinal());
            }
        };

        /**
         * code: 2
         * name: com.alibaba.amp.any.common.record.formats.avro.Character
         */
        valueDeserializers[2] = new ValueDeserializer<DStoreRawValue>() {
            @Override
            public DStoreRawValue deserialize(Decoder decoder, int sourceTypeCode) throws IOException {
                String charset = decoder.readString();
                ByteBuffer data = decoder.readBytes(null);
                //return new StringValue(data, charset);
                return new DStoreRawValue(new ByteString(data.array()), charset, DataMessage.Record.Field.Type.STRING.ordinal());
            }
        };

        /**
         * code: 3
         * name: com.alibaba.amp.any.common.record.formats.avro.Decimal
         */
        valueDeserializers[3] = new ValueDeserializer<DStoreRawValue>() {
            @Override
            public DStoreRawValue deserialize(Decoder decoder, int sourceTypeCode) throws IOException {
                String text = decoder.readString();
                decoder.readInt();
                decoder.readInt();
                try {
                    //return new DecimalNumeric(text);
                    return new DStoreRawValue(new ByteString(text.getBytes()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.DECIMAL.ordinal());
                } catch (NumberFormatException ex) {
                    //return new SpecialNumeric(SpecialNumeric.SpecialNumericType.parseFrom(text));
                    return new DStoreRawValue(new ByteString(text.getBytes()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.STRING.ordinal());
                }
            }
        };

        /**
         * code: 4
         * name: com.alibaba.amp.any.common.record.formats.avro.Float
         */
        valueDeserializers[4] = new ValueDeserializer<DStoreRawValue>() {
            @Override
            public DStoreRawValue deserialize(Decoder decoder, int sourceTypeCode) throws IOException {
                Double doubleValue = decoder.readDouble();
                //FloatNumeric numeric = new FloatNumeric(decoder.readDouble());
                decoder.readInt();
                decoder.readInt();
                //return numeric;
                return new DStoreRawValue(new ByteString(doubleValue.toString().getBytes()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.DOUBLE.ordinal());
            }
        };

        /**
         * code: 5
         * name: com.alibaba.amp.any.common.record.formats.avro.Timestamp
         */
        valueDeserializers[5] = new ValueDeserializer<DStoreRawValue>() {
            @Override
            public DStoreRawValue deserialize(Decoder decoder, int sourceTypeCode) throws IOException {
                UnixTimestamp unixTimestamp = new UnixTimestamp(decoder.readLong(), decoder.readInt());
                return new DStoreRawValue(new ByteString(unixTimestamp.toString().getBytes()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.TIMESTAMP.ordinal());
            }
        };

        /**
         * code: 6
         * name: com.alibaba.amp.any.common.record.formats.avro.DateTime
         */
        valueDeserializers[6] = new ValueDeserializer<DStoreRawValue>() {
            @Override
            public DStoreRawValue deserialize(Decoder decoder, int sourceTypeCode) throws IOException {
                DateTime dateTime = deserializeDateTime(decoder, sourceTypeCode);
                return new DStoreRawValue(new ByteString(dateTime.toString().getBytes()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.DATETIME.ordinal());
            }
        };

        /**
         * code: 7
         * name: com.alibaba.amp.any.common.record.formats.avro.TimestampWithTimeZone
         */
        valueDeserializers[7] = new ValueDeserializer<DStoreRawValue>() {
            @Override
            public DStoreRawValue deserialize(Decoder decoder, int sourceTypeCode) throws IOException {
                DateTime dateTime = deserializeDateTime(decoder, sourceTypeCode);

                String timeZone = decoder.readString();
                dateTime.setSegments(DateTime.SEG_TIMEZONE);
                if (SourceType.PostgreSQL.ordinal() == sourceTypeCode) {
                    timeZone = "GMT" + timeZone;
                }
                dateTime.setTimeZone(timeZone);
                return new DStoreRawValue(new ByteString(dateTime.toString().getBytes()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.DATETIME.ordinal());
            }
        };

        /**
         * code: 8
         * name: com.alibaba.amp.any.common.record.formats.avro.BinaryGeometry
         */
        valueDeserializers[8] = new ValueDeserializer<DStoreRawValue>() {
            @Override
            public DStoreRawValue deserialize(Decoder decoder, int sourceTypeCode) throws IOException {
                // skip type string
                decoder.skipString();
                //WKBGeometry wkbGeometry = new WKBGeometry(decoder.readBytes(null));
                return new DStoreRawValue(new ByteString(decoder.readBytes(null).array()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.GEOMETRY.ordinal());
            }
        };

        /**
         * code: 9
         * name: com.alibaba.amp.any.common.record.formats.avro.TextGeometry
         */
        valueDeserializers[9] = new ValueDeserializer<DStoreRawValue>() {
            @Override
            public DStoreRawValue deserialize(Decoder decoder, int sourceTypeCode) throws IOException {
                // skip type string
                decoder.skipString();
                //WKTGeometry wktGeometry = new WKTGeometry(decoder.readString());
                return new DStoreRawValue(new ByteString(decoder.readString().getBytes()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.GEOMETRY.ordinal());
            }
        };

        /**
         * code: 10
         * name: com.alibaba.amp.any.common.record.formats.avro.BinaryObject
         */
        valueDeserializers[10] = new ValueDeserializer<DStoreRawValue>() {
            @Override
            public DStoreRawValue deserialize(Decoder decoder, int sourceTypeCode) throws IOException {
                BinaryEncodingObject binary =  new BinaryEncodingObject(ObjectType.valueOf(decoder.readString().toUpperCase()), decoder.readBytes(null));
                switch (binary.getObjectType()) {
                    case SET:
                        return new DStoreRawValue(new ByteString(binary.getData().array()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.SET.ordinal());
                    case TEXT:
                    case BLOB:
                        return new DStoreRawValue(new ByteString(binary.getData().array()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.BLOB.ordinal());
                    case ENUM:
                        return new DStoreRawValue(new ByteString(binary.getData().array()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.ENUM.ordinal());
                    case JSON:
                        return new DStoreRawValue(new ByteString(binary.getData().array()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.JSON.ordinal());
                    case GEOMETRY:
                        return new DStoreRawValue(new ByteString(binary.getData().array()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.GEOMETRY.ordinal());
                    case BINARY:
                        return new DStoreRawValue(new ByteString(binary.getData().array()), BINARY_ENCODE, DataMessage.Record.Field.Type.STRING.ordinal());
                    case UROWID:
                    case LONG_RAW:
                    case XTYPE:
                    case ROWID:
                    case BYTEA:
                    case BFILE:
                    case XML:
                    case RAW:
                    default:
                        return new DStoreRawValue(new ByteString(binary.getData().array()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.STRING.ordinal());
                }
            }
        };

        /**
         * code: 11
         * name: com.alibaba.amp.any.common.record.formats.avro.TextObject
         */
        valueDeserializers[11] = new ValueDeserializer<DStoreRawValue>() {
            @Override
            public DStoreRawValue deserialize(Decoder decoder, int sourceTypeCode) throws IOException {
                return valueDeserializers[10].deserialize(decoder, sourceTypeCode);
            }
        };

        /**
         * code: 12
         * name: com.alibaba.amp.any.common.record.formats.avro.EmptyObject
         */
        valueDeserializers[12] = new ValueDeserializer<DStoreRawValue>() {
            @Override
            public DStoreRawValue deserialize(Decoder decoder, int sourceTypeCode) throws IOException {
                decoder.readEnum();
                return null;
            }
        };
    }

    public static void deserializeHeader(Decoder decoder, DStoreAvroRecord record) throws IOException {
        // skip version
        decoder.readInt();
        record.setId(decoder.readLong());
        record.setTimestamp(decoder.readLong());
        record.setCheckpoint(decoder.readString());
        //sourceSafePosition
        decoder.readString();
        record.setTransactionId(decoder.readString());
        // skip safeSourcePosition
        Pair<Integer, String> dbInfoPair = deserializeSource(decoder);
        record.setSrcType(dbInfoPair.getKey());
        record.setVersion(dbInfoPair.getRight());
        int op = decoder.readEnum();
        record.setOpt(operationDeserializers[op]);
        record.setRawOp(op);
        int unionIndex = decoder.readIndex();
        if (0 == unionIndex) {
            decoder.readNull();

        } else if (1 == unionIndex) {
            // parse db/schema/tb name
            String objectName = decoder.readString();
            Triple<String, String, String> nameTriple = deserializeNameTriple(objectName);
            record.setSchema(new DStoreRecordSchema(record, nameTriple.toString(), nameTriple.getLeft(), nameTriple.getRight()));
        }
    }

    public static void deserializePayload(Decoder decoder, DStoreAvroRecord record) throws IOException {
        //skip Long list
        deserializeLongList(decoder, true);
        //extendedProperty
        Map<String, String> tags = deserializeMap(decoder);
        deserializeFieldListAndIndex(decoder, record, tags.get("pk_uk_info"));
        record.setThreadId(tags.get("thread_id"));
        record.setTraceId(tags.get("traceid"));
        record.setBeforeImage(deserializeRowImage(decoder, record.getSchema(false), record.getSrcType()));
        record.setAfterImage(deserializeRowImage(decoder, record.getSchema(false), record.getSrcType()));
    }

    protected static Pair<Integer, String> deserializeSource(Decoder decoder)
            throws IOException {
        return Pair.of(decoder.readEnum(), decoder.readString());
    }

    protected static Triple<String, String, String> deserializeNameTriple(String mixedNames) {
        String dbName = null;
        String schemaName = null;
        String tableName = null;

        String[] dbPair = ObjectNameUtils.uncompressionObjectName(mixedNames);
        if (dbPair != null) {
            if (dbPair.length < 1 || dbPair.length > 3) {
                throw new RuntimeException("Invalid db table name pair for mixed [" + mixedNames + "]");
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
        Map<String, String> map = new LinkedHashMap<String, String>();
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

    protected static Pair<Map<String, Boolean>, Map<String, Pair<Boolean, List<String>>>> deserializePkUkInfo(String text) {
        Map<String, Pair<Boolean, List<String>>> pkUkInfo = new LinkedHashMap<String, Pair<Boolean, List<String>>>();
        Map<String, Boolean> columnUkInfo = new HashMap<String, Boolean>();
        if (StringUtils.isNotEmpty(text)) {
            JSONObject jsonObject = JSON.parseObject(text);
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                String key = entry.getKey();
                JSONArray jsonArray = (JSONArray) entry.getValue();
                boolean isPrimaryKey = StringUtils.equals("PRIMARY", key);
                List<String> columns = new ArrayList<String>();
                for (Object value : jsonArray) {
                    String colName = (String) value;
                    columns.add(colName);
                    if (isPrimaryKey || (!columnUkInfo.containsKey(colName))) {
                        columnUkInfo.put(colName, isPrimaryKey);
                    }
                }
                pkUkInfo.put(key, Pair.of(isPrimaryKey, columns));
            }
        }
        return ImmutablePair.of(columnUkInfo, pkUkInfo);
    }

    protected static void deserializeFieldListAndIndex(Decoder decoder,
                                                       DStoreAvroRecord record,
                                                       String pkukInfo) throws IOException {
        // parse pk/uk names
        Pair<Map<String, Boolean>, Map<String, Pair<Boolean, List<String>>>> pkUkInfo = deserializePkUkInfo(pkukInfo);

        DStoreRecordSchema recordSchema = record.getSchema(false);
        int fieldIndex = decoder.readIndex();
        if (2 == fieldIndex) {

            List<RecordField> recordFields = deserializeFieldList(decoder, pkUkInfo.getKey());
            recordSchema.setRecordFields(recordFields);
            // build uk index infos
            Map<String, Pair<Boolean, List<String>>> pkUkInfoMap = pkUkInfo.getRight();
            for (Map.Entry<String, Pair<Boolean, List<String>>> entry : pkUkInfoMap.entrySet()) {

                boolean isPrimaryKey = entry.getValue().getLeft();
                RecordIndexInfo recordIndexInfo = new RecordIndexInfo(
                        isPrimaryKey ? RecordIndexInfo.IndexType.PrimaryKey : RecordIndexInfo.IndexType.UniqueKey);
                for (String ukFieldName : entry.getValue().getRight()) {
                    RecordField f = recordSchema.getField(ukFieldName, false);
                    if (null == f) {
                        throw new RuntimeException(ukFieldName + " not found in record [" + recordSchema.getFullQualifiedName() + "]");
                    }
                    recordIndexInfo.addField(f);
                }
                if (isPrimaryKey) {
                    recordSchema.setPrimaryIndexInfo(recordIndexInfo);
                } else {
                    recordSchema.addUniqueIndexInfo(recordIndexInfo);
                }
            }
        } else if (1 == fieldIndex) {
            decoder.readString();
        } else {
            decoder.readNull();
            DStoreRecordSchema ddlRecordSchema = record.getSchema(false);
            if (DataMessage.Record.Type.DDL == record.getOpt()) {
                if (null == ddlRecordSchema) {
                    ddlRecordSchema = new DStoreRecordSchema(record, null, null, null);
                    record.setSchema(ddlRecordSchema);
                }
                RecordField ddlField = new SimplifiedRecordField("ddl", DefaultRawDataType.of(0));
                ddlRecordSchema.setRecordFields(Arrays.asList(ddlField));
            }
        }
    }

    protected static List<RecordField> deserializeFieldList(Decoder decoder, Map<String, Boolean> colPkUkInfo) throws IOException {
        long chunkLen = (decoder.readArrayStart());
        List<RecordField> recordFields = new ArrayList<RecordField>();
        int fieldPosition = 0;
        if (chunkLen > 0) {
            do {
                for (int counter = 0; (counter < chunkLen); ++counter) {
                    String fieldName = decoder.readString();
                    int fieldTypeNumber = decoder.readInt();
                    SimplifiedRecordField recordField = new SimplifiedRecordField(fieldName, DefaultRawDataType.of(fieldTypeNumber));
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
        return recordFields;
    }

    protected static DStoreRowImage deserializeRowImage(Decoder decoder, DStoreRecordSchema recordSchema, int souceTypeCode) throws IOException {
        DStoreRowImage rowImage = null;
        int imageIndex = decoder.readIndex();
        if (2 == imageIndex) {
            long chunkLen = (decoder.readArrayStart());
            if (chunkLen > 0) {
                rowImage = new DStoreRowImage(recordSchema, recordSchema.getFieldCount(false));
                int index = 0;
                do {
                    for (int counter = 0; (counter < chunkLen); counter++) {
                        int type = (decoder.readIndex());
                        rowImage.setValue(index, valueDeserializers[type].deserialize(decoder, souceTypeCode));
                        ++index;
                    }
                    chunkLen = (decoder.arrayNext());
                } while (chunkLen > 0);
            }
        } else if (1 == imageIndex) {
            rowImage = new DStoreRowImage(recordSchema, recordSchema.getFieldCount(false));
            rowImage.setValue(0, new DStoreRawValue(new ByteString(decoder.readString().getBytes()), BytesUtil.UTF8_ENCODING, DataMessage.Record.Field.Type.STRING.ordinal()));
        } else {
            decoder.readNull();
            return null;
        }
        return rowImage;
    }
}
