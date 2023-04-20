package com.taobao.drc.togo.data;

import com.taobao.drc.togo.data.schema.Schema;
import com.taobao.drc.togo.data.schema.Type;

import java.util.Objects;

/**
 * @author yangyang
 * @since 17/3/29
 */
public class SchemaAndValue {
    private final int schemaId;
    private final int schemaVersion;
    private final Schema schema;
    private final Object value;

    public SchemaAndValue(int schemaId, int schemaVersion, Schema schema, Object value) {
        this.schemaId = schemaId;
        this.schemaVersion = schemaVersion;
        this.schema = schema;
        this.value = value;
    }

    public SchemaAndValue(SchemaAndValue that, Object value) {
        this(that.schemaId, that.schemaVersion, that.schema, value);
    }

    public int getSchemaId() {
        return schemaId;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public Schema getSchema() {
        return schema;
    }

    public Object getValue() {
        return value;
    }

    public boolean isRecord() {
        return schema.type() == Type.STRUCT;
    }

    public SchemafulRecord getValueAsRecord() {
        if (!isRecord()) {
            throw new DataException("The type of schema is not record, actually is: [" + schema.type() + "]");
        }
        if (value instanceof SchemafulRecord) {
            return (SchemafulRecord) value;
        } else {
            throw new DataException("The type of [" + value + "] is not record");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaAndValue that = (SchemaAndValue) o;
        return schemaId == that.schemaId &&
                schemaVersion == that.schemaVersion &&
                Objects.equals(schema, that.schema) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaId, schemaVersion, schema, value);
    }

    @Override
    public String toString() {
        return "SchemaAndValue{" +
                "schemaId=" + schemaId +
                ", schemaVersion=" + schemaVersion +
                ", schema=" + schema +
                ", value=" + value +
                '}';
    }
}
