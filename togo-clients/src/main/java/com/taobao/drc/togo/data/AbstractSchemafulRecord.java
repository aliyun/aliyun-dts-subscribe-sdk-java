package com.taobao.drc.togo.data;

import com.taobao.drc.togo.data.schema.Field;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author yangyang
 * @since 17/4/12
 */
public abstract class AbstractSchemafulRecord implements SchemafulRecord {
    protected int getFieldIndex(String name) {
        Field field = getSchema().field(name);
        if (field == null) {
            throw new DataException("Field [" + name + "] not exists in schema [" + getSchema() + "]");
        }
        return field.index();
    }

    @Override
    public int hashCode() {
        int result = 0;
        if (getSchema() != null) {
            result = getSchema().hashCode();
        }
        for (int i = 0; i < getSchema().fields().size(); i++) {
            Object v = get(i);
            result = 31 * result;
            if (v != null) {
                if (v instanceof byte[]) {
                    result += Arrays.hashCode((byte[]) v);
                } else if (v instanceof Object[]) {
                    result += Arrays.deepHashCode((Object[]) v);
                } else {
                    result += v.hashCode();
                }
            }
        }
        return result;
    }

    private ByteBuffer wrapBytes(Object v) {
        if (v instanceof ByteBuffer) {
            return (ByteBuffer) v;
        } else if (v instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) v);
        } else {
            throw new IllegalArgumentException("Illegal value for BYTES [" + v + "]");
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof AbstractSchemafulRecord)) {
            return false;
        }
        if (!Objects.equals(getSchema(), ((AbstractSchemafulRecord) obj).getSchema())) {
            return false;
        }
        for (Field field : getSchema().fields()) {
            Object v1 = get(field.index());
            Object v2 = ((AbstractSchemafulRecord) obj).get(field.index());
            if (v1 == null || v2 == null) {
                if (v1 == v2) {
                    continue;
                } else {
                    return false;
                }
            }
            switch (field.schema().type()) {
                case BYTES:
                    if (!wrapBytes(v1).equals(wrapBytes(v2))) {
                        return false;
                    }
                    break;
                case ARRAY:
                    if (!v1.equals(v2)) {
                        return false;
                    }
                    break;
                case STRING:
                    if (!v1.toString().equals(v2.toString())) {
                        return false;
                    }
                    break;
                default:
                    if (!v1.equals(v2)) {
                        return false;
                    }
            }
        }
        return true;
    }

    private String valueToString(Object val) {
        if (val instanceof byte[]) {
            return Arrays.toString((byte[]) val);
        } else {
            return val.toString();
        }
    }

    @Override
    public String toString() {
        return getSchema().fields().stream()
                .map(f -> f.name() + "=" + valueToString(get(f.index())))
                .collect(Collectors.joining(", ", "SchemafulRecord{", "}"));
    }
}
