package com.taobao.drc.togo.data.schema.impl;

import com.taobao.drc.togo.data.schema.Field;
import com.taobao.drc.togo.data.schema.Schema;
import com.taobao.drc.togo.data.schema.SchemaException;
import com.taobao.drc.togo.data.schema.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author yangyang
 * @since 17/3/9
 */
public class DefaultSchema implements Schema {
    private final String name;
    private final String doc;
    private final Type type;
    private final Schema valueSchema;
    private final List<Field> fields;
    private final String logicalType;
    private final Map<String, String> parameters;
    private final boolean optional;
    private final Object defaultValue;
    private final Map<String, Field> schemaNameMap;
    private final int hashCode;

    public DefaultSchema(String name, String doc, Type type, Schema valueSchema,
                         List<Field> fields,
                         String logicalType, Map<String, String> parameters,
                         boolean optional, Object defaultValue) {
        this.name = name;
        this.doc = doc;
        this.type = type;
        this.valueSchema = valueSchema;
        this.fields = fields;
        this.logicalType = logicalType;
        this.parameters = parameters;
        this.optional = optional;
        this.defaultValue = defaultValue;
        if (type == Type.STRUCT) {
            schemaNameMap = buildFieldNameMap();
        } else {
            schemaNameMap = null;
        }

        hashCode = calculateHashCode();
    }

    private Map<String, Field> buildFieldNameMap() {
        Map<String, Field> ret = new HashMap<>();
        for (Field field : fields) {
            ret.put(field.name(), field);
        }
        return ret;
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String doc() {
        return doc;
    }

    @Override
    public Schema valueSchema() {
        return valueSchema;
    }

    @Override
    public String logicalType() {
        return logicalType;
    }

    @Override
    public Map<String, String> parameters() {
        return parameters;
    }

    @Override
    public boolean isOptional() {
        return optional;
    }

    @Override
    public Object defaultValue() {
        return defaultValue;
    }

    @Override
    public Field field(String name) {
        if (type() != Type.STRUCT) {
            throw new SchemaException("Schema [" + this + "] is not a struct");
        }
        return schemaNameMap.get(name);
    }

    @Override
    public List<Field> fields() {
        if (type == Type.STRUCT)
            return fields;
        else
            throw new SchemaException(toString() + " is not a structure");
    }

    @Override
    public String toString() {
        return "DefaultSchema{" +
                "name='" + name + '\'' +
                ", doc='" + doc + '\'' +
                ", type=" + type +
                ", valueSchema=" + valueSchema +
                ", fields=" + fields +
                ", logicalType='" + logicalType + '\'' +
                ", parameters=" + parameters +
                ", optional=" + optional +
                ", defaultValue=" + defaultValue +
                ", schemaNameMap=" + schemaNameMap +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultSchema that = (DefaultSchema) o;
        return optional == that.optional &&
                Objects.equals(name, that.name) &&
                Objects.equals(doc, that.doc) &&
                type == that.type &&
                Objects.equals(valueSchema, that.valueSchema) &&
                Objects.equals(fields, that.fields) &&
                Objects.equals(logicalType, that.logicalType) &&
                Objects.equals(parameters, that.parameters) &&
                Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int calculateHashCode() {
        return Objects.hash(name, doc, type, valueSchema, fields, logicalType, parameters, optional, defaultValue, schemaNameMap);
    }
}
