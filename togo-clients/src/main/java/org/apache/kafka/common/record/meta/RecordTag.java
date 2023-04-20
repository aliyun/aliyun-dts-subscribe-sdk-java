package org.apache.kafka.common.record.meta;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.util.List;

public final class RecordTag extends RecordMeta {
    private static final String NAME_KEY_NAME = "name";
    private static final String REFERENCE_SCHEMA_KEY_NAME = "record_schema";
    private static final String REFERENCE_FIELD_KEY_NAME = "record_fields";
    private static final String TYPE_KEY_NAME = "type";
    private static final String FIELDS_MAP_TYPE_KEY_NAME = "fields_map_type";

    public final String name;
    public final String schemaName;
    public final RecordTagType tagType;
    public final RecordTagFieldsMapType fieldsMapType;
    public final List<String> fieldNames;

    public RecordTag() {
        this("");
    }

    public RecordTag(String name) {
        this(name, "", RecordTagType.UNKNOWN, RecordTagFieldsMapType.UNKNOWN, null);
    }

    public RecordTag(String name, Errors error) {
        this(name);
        setError(error);
    }

    public RecordTag(String name, String schemaName, RecordTagType tagType, RecordTagFieldsMapType fieldsMapType, List<String> fieldNames) {
        this.name = name;
        this.schemaName = schemaName;
        this.tagType = tagType;
        this.fieldsMapType = fieldsMapType;
        this.fieldNames = fieldNames;
    }

    public static RecordTag parseFrom(Struct struct) {
        return new RecordTag(
                struct.getString(NAME_KEY_NAME),
                struct.getString(REFERENCE_SCHEMA_KEY_NAME),
                RecordTagType.valueOf(struct.getShort(TYPE_KEY_NAME)),
                RecordTagFieldsMapType.valueOf(struct.getShort(FIELDS_MAP_TYPE_KEY_NAME)),
                toStringList(struct.getArray(REFERENCE_FIELD_KEY_NAME)));
    }

    @Override
    public Struct toStruct(String keyName, Struct parent) {
        Struct ret = parent.instance(keyName);
        ret.set(NAME_KEY_NAME, name);
        ret.set(TYPE_KEY_NAME, tagType.id);
        ret.set(REFERENCE_SCHEMA_KEY_NAME, schemaName);
        ret.set(FIELDS_MAP_TYPE_KEY_NAME, fieldsMapType.id);
        if (null != fieldNames) {
            ret.set(REFERENCE_FIELD_KEY_NAME, fieldNames.toArray());
        }
        return ret;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("(name=").append(name).
                append(", schemaName=").append(schemaName).
                append(", tagType=").append(tagType).
                append(", fieldsMapType=").append(fieldsMapType).
                append(", fieldNames=").append(Utils.mkString(fieldNames, ",")).
                append(")");
        return bld.toString();
    }

    public enum RecordTagType {
        UNKNOWN(0, "Unknown"),
        FILTER_TAG(1, "FilterTag"),
        INDEX_TAG(2, "IndexTag");

        public final short id;
        public final String name;

        RecordTagType(int id, String name) {
            this.id = (short) id;
            this.name = name;
        }

        static RecordTagType valueOf(int id) {
            switch (id) {
                case 1:
                    return FILTER_TAG;
                case 2:
                    return INDEX_TAG;
                default:
                    throw new AssertionError(String.format("invalid RecordTagType value %d", id));
            }
        }
    }

    public enum RecordTagFieldsMapType {
        UNKNOWN(0, "Unknown"),
        RAW_MAP(1, "RawMap"),
        ID_MAP(2, "IdMap");

        public final short id;
        public final String name;

        RecordTagFieldsMapType(int id, String name) {
            this.id = (short)id;
            this.name = name;
        }

        static RecordTagFieldsMapType valueOf(int id) {
            switch(id) {
                case 1:
                    return RAW_MAP;
                case 2:
                    return ID_MAP;
                default:
                    throw new AssertionError(String.format("invalid RecordTagFieldsMapType value %d", id));
            }
        }
    }
}
