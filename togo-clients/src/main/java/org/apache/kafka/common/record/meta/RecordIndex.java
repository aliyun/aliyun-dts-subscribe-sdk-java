package org.apache.kafka.common.record.meta;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.util.List;

public final class RecordIndex extends RecordMeta {
    private static final String NAME_KEY_NAME = "name";
    private static final String REFERENCE_SCHEMA_KEY_NAME = "record_schema";
    private static final String REFERENCE_FIELD_KEY_NAME = "record_fields";

    public final String name;
    public final String schemaName;
    public final List<String> schemaFields;

    public RecordIndex() {
        this("");
    }

    public RecordIndex(String name) {
        this(name, "", null);
    }

    public RecordIndex(String name, String schemaName, List<String> schemaFields) {
        this.name = name;
        this.schemaName = schemaName;
        this.schemaFields = schemaFields;
    }

    public RecordIndex(Errors error) {
        this();
        setError(error);
    }

    public RecordIndex(String name, Errors error) {
        this(name);
        setError(error);
    }

    public static RecordIndex parseFrom(Struct struct) {
        return new RecordIndex(
                struct.getString(NAME_KEY_NAME),
                struct.getString(REFERENCE_SCHEMA_KEY_NAME),
                toStringList(struct.getArray(REFERENCE_FIELD_KEY_NAME)));
    }

    @Override
    public Struct toStruct(String keyName, Struct parent) {
        Struct ret = parent.instance(keyName);
        ret.set(NAME_KEY_NAME, name);
        ret.set(REFERENCE_SCHEMA_KEY_NAME, schemaName);
        if (null != schemaFields) {
            ret.set(REFERENCE_FIELD_KEY_NAME, schemaFields.toArray());
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
                append(", schemaFields=").append(Utils.mkString(schemaFields, ",")).
                append(")");
        return bld.toString();
    }
}
