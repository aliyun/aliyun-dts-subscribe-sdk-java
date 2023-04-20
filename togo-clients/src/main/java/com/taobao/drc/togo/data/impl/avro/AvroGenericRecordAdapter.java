package com.taobao.drc.togo.data.impl.avro;

import com.taobao.drc.togo.data.AbstractSchemafulRecord;
import com.taobao.drc.togo.data.schema.Field;
import com.taobao.drc.togo.data.schema.Schema;
import com.taobao.drc.togo.data.schema.Type;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;

/**
 * @author yangyang
 * @since 17/3/20
 */
public class AvroGenericRecordAdapter extends AbstractSchemafulRecord {
    private final GenericRecord innerRecord;
    private final Schema schema;

    public AvroGenericRecordAdapter(GenericRecord innerRecord, Schema schema) {
        this.innerRecord = innerRecord;
        this.schema = schema;
    }

    public GenericRecord getInnerRecord() {
        return innerRecord;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public Object get(String key) {
        return get(getFieldIndex(key));
    }

    @Override
    public AvroGenericRecordAdapter put(String key, Object t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(int index) {
        return mayBeWrapReturnedObj(innerRecord.get(index), schema.fields().get(index));
    }

    @Override
    public AvroGenericRecordAdapter put(int index, Object t) {
        throw new UnsupportedOperationException();
    }

    private Object mayBeWrapReturnedObj(Object object, Field field) {
        if (field.schema().type() == Type.STRING) {
            return object.toString();
        } else if (field.schema().type() == Type.BYTES) {
            return ((ByteBuffer) object).array();
        } else {
            return object;
        }
    }
}
