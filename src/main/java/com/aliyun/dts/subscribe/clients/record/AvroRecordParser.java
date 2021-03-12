package com.aliyun.dts.subscribe.clients.record;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSON;

import com.aliyun.dts.subscribe.clients.exception.DTSBaseException;
import com.aliyun.dts.subscribe.clients.formats.avro.DefaultValueDeserializer;
import com.aliyun.dts.subscribe.clients.formats.avro.Field;
import com.aliyun.dts.subscribe.clients.formats.avro.Operation;
import com.aliyun.dts.subscribe.clients.formats.avro.Record;
import com.aliyun.dts.subscribe.clients.formats.util.ObjectNameUtils;
import com.aliyun.dts.subscribe.clients.record.impl.DefaultRowImage;
import com.aliyun.dts.subscribe.clients.record.value.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.*;
import java.util.stream.Collectors;

public class AvroRecordParser {

    public static OperationType getOperationType(Record avroRecord) {
        switch (avroRecord.getOperation()) {
            case INSERT:
                return OperationType.INSERT;
            case UPDATE:
                return OperationType.UPDATE;
            case DELETE:
                return OperationType.DELETE;
            case DDL:
                return OperationType.DDL;
            case HEARTBEAT:
                return OperationType.HEARTBEAT;
            case CHECKPOINT:
                return OperationType.CHECKPOINT;
            case BEGIN:
                return OperationType.BEGIN;
            case COMMIT:
                return OperationType.COMMIT;
            default:
                return OperationType.UNKNOWN;
        }
    }

    private static Triple<String, String, String> getNames(String mixedNames) {
        String dbName = null;
        String schemaName = null;
        String tableName = null;

        String[] dbPair = ObjectNameUtils.uncompressionObjectName(mixedNames);
        if (dbPair != null) {
            if (dbPair.length == 2) {
                dbName = dbPair[0];
                tableName = dbPair[1];
                schemaName = dbPair[0];
            } else if (dbPair.length == 3) {
                dbName = dbPair[0];
                schemaName = dbPair[1];
                tableName = dbPair[2];
            } else if (dbPair.length == 1) {
                dbName = dbPair[0];
            } else {
                throw new DTSBaseException("Invalid db table name pair for mixed [" + mixedNames + "]");
            }
        }

        return Triple.of(dbName, schemaName, tableName);
    }

    private static Pair<Set<String>, List<Set<String>>> getPrimaryAndUniqueKeyNames(Record avroRecord) {
        Set<String> pkNameSet = new TreeSet<>();
        List<Set<String>> ukNameList = new LinkedList<>();

        String pkUkInfoStr = avroRecord.getTags().get("pk_uk_info");
        if (!StringUtils.isEmpty(pkUkInfoStr)) {
            JSONObject jsonObject = JSON.parseObject(pkUkInfoStr);
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                String key = entry.getKey();
                JSONArray jsonArray = (JSONArray) entry.getValue();
                if (StringUtils.equals("PRIMARY", key)) {
                    for (Object value : jsonArray) {
                        pkNameSet.add(String.valueOf(value));
                    }
                } else {
                    Set<String> ukNameSet = new TreeSet<>();
                    for (Object value : jsonArray) {
                        ukNameSet.add(String.valueOf(value));
                    }
                    ukNameList.add(ukNameSet);
                }
            }
        }
        return Pair.of(pkNameSet, ukNameList);
    }

    private static List<Field> getAvroRecordFields(Record avroRecord) {
        List<Field> fields = (List<Field>) avroRecord.getFields();

        if (null == fields && Operation.DDL == avroRecord.getOperation()) {
            Field fakeField = new Field();
            fakeField.setName("ddl");
            fakeField.setDataTypeNumber(0);
            fields = Arrays.asList(fakeField);
        }

        if (null == fields) {
            fields = Collections.emptyList();
        }

        return fields;
    }

    //Cache may be introduced to this method to reduce repeat schema parse
    private static Pair<List<RecordField>, RecordIndexInfo> getRecordFields(Record avroRecord, Set<String> pkNames, Set<String> ukNames) {
        List<Field> fields = getAvroRecordFields(avroRecord);
        List<RecordField> recordFields = new ArrayList<>(fields.size());
        RecordIndexInfo pkOrUkIndexInfo = new RecordIndexInfo(RecordIndexInfo.IndexType.PrimaryKey);
        int fieldPosition = 0;
        for (Field field : fields) {
            final String filedName = field.getName();
            SimplifiedRecordField recordField = new SimplifiedRecordField(filedName, field.getDataTypeNumber());

            boolean pkOrUk = recordField.setPrimary(pkNames.contains(filedName));
            recordField.setUnique(ukNames.contains(filedName));
            if (pkOrUk) {
                pkOrUkIndexInfo.addField(recordField);
            }
            recordField.setFieldPosition(fieldPosition);
            recordFields.add(recordField);

            fieldPosition++;
        }
        return Pair.of(recordFields, pkOrUkIndexInfo);
    }

    public static RecordSchema getRecordSchema(Record avroRecord) {
        // parse db/schema/tb name
        Triple<String, String, String> names = getNames(avroRecord.getObjectName());

        // parse pk/uk names
        Pair<Set<String>, List<Set<String>>> keyNamePair = getPrimaryAndUniqueKeyNames(avroRecord);
        Set<String> pkNames = keyNamePair.getLeft();
        Set<String> allUkNames = keyNamePair.getRight().stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        // parse record fields
        Pair<List<RecordField>, RecordIndexInfo> recordFieldAndIndexInfo = getRecordFields(avroRecord, pkNames, allUkNames);

        // compose record schema
        String schemaId = names.toString();
        List<RecordField> recordFields = recordFieldAndIndexInfo.getLeft();
        RecordIndexInfo pkOrUkIndexInfo = recordFieldAndIndexInfo.getRight();
        DefaultRecordSchema recordSchema = new DefaultRecordSchema(schemaId, names.getLeft(), names.getRight(), recordFields);

        //db type and version
        recordSchema.setDatabaseInfo(new DatabaseInfo(avroRecord.getSource().getSourceType().name(), avroRecord.getSource().getVersion()));

        if (!pkOrUkIndexInfo.getIndexFields().isEmpty()) {
            recordSchema.setPrimaryIndexInfo(pkOrUkIndexInfo);
        }
        // build uk index infos
        for (Set<String> ukNameSet : keyNamePair.getRight()) {
            RecordIndexInfo recordIndexInfo = new RecordIndexInfo(RecordIndexInfo.IndexType.UniqueKey);
            for (String ukFieldName : ukNameSet) {
                recordIndexInfo.addField(recordSchema.getField(ukFieldName).orElseThrow(() -> new RuntimeException(ukFieldName + " not found in record [" + avroRecord + "]")));
            }
            recordSchema.addUniqueIndexInfo(recordIndexInfo);
        }

        return recordSchema;
    }

    public static RowImage getRowImage(RecordSchema recordSchema, Record avroRecord, boolean isBefore) {
        List<Object> rowImage;

        if (isBefore) {
            rowImage = (List<Object>) avroRecord.getBeforeImages();
        } else {
            Object avroRowImage = avroRecord.getAfterImages();
            if (null == avroRowImage) {
                rowImage = null;
            } else if (avroRowImage instanceof String) {
                rowImage = Arrays.asList(avroRowImage.toString());
            } else {
                rowImage = (List<Object>) avroRecord.getAfterImages();
            }
        }

        DefaultRowImage rs = null;

        if (rowImage != null && !rowImage.isEmpty()) {
            rs = new DefaultRowImage(recordSchema);
            int index = 0;
            for (Object rowValue : rowImage) {
                Value value = DefaultValueDeserializer.deserialize(rowValue);
                rs.setValue(index, value);
                index++;
            }
        }

        return rs;
    }
}
