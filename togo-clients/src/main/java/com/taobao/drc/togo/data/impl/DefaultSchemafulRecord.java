package com.taobao.drc.togo.data.impl;

import com.taobao.drc.togo.data.AbstractSchemafulRecord;
import com.taobao.drc.togo.data.DataException;
import com.taobao.drc.togo.data.schema.*;

import java.util.Objects;

/**
 * @author yangyang
 * @since 17/3/21
 */
public class DefaultSchemafulRecord extends AbstractSchemafulRecord {
    private final Schema schema;
    private final Object[] values;

    public DefaultSchemafulRecord(Schema schema) {
        Objects.requireNonNull(schema);
        if (schema.type() != Type.STRUCT) {
            throw new DataException("The type of schema is not struct: " + schema);
        }
        this.schema = schema;
        values = new Object[schema.fields().size()];
    }

    public void validate() {
        for (Field field : schema.fields()) {
            Schema fieldSchema = field.schema();
            Object value = values[field.index()];
            if (value == null && (fieldSchema.isOptional() || fieldSchema.defaultValue() != null)) {
                continue;
            }
            try {
                SchemaBuilder.validateValue(fieldSchema, value);
            } catch (SchemaBuilderException e) {
                throw new DataException("Failed to validate the value [" + value + "] of the field [" + field + "]", e);
            }
        }
    }

    @Override
    public Object get(String key) {
        return get(getFieldIndex(key));
    }

    @Override
    public DefaultSchemafulRecord put(String key, Object t) {
        put(getFieldIndex(key), t);
        return this;
    }

    @Override
    public Object get(int index) {
        Field field = schema.fields().get(index);
        Object value = values[index];
        if (value == null && field.schema().defaultValue() != null) {
            value = field.schema().defaultValue();
        }
        return value;
    }

    @Override
    public DefaultSchemafulRecord put(int index, Object t) {
        Field field = schema.fields().get(index);
        try {
            SchemaBuilder.validateValue(field.schema(), t);
        } catch (SchemaBuilderException e) {
            throw new DataException("Failed to set value [" + t + "] to field [" + field + "]", e);
        }
        values[index] = t;
        return this;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
