package com.taobao.drc.togo.data.impl;

import com.taobao.drc.togo.data.DataDeserializer;
import com.taobao.drc.togo.data.SchemaAndValue;
import com.taobao.drc.togo.data.SchemaNotFoundException;
import com.taobao.drc.togo.data.impl.avro.AvroDataDeserializer;
import com.taobao.drc.togo.data.impl.avro.AvroDataSerDeConstants;
import com.taobao.drc.togo.data.schema.SchemaCache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yangyang
 * @since 17/3/20
 */
public class IntegratedDataDeserializer implements DataDeserializer {
    private final List<DataDeserializer> supportedDeserializers = new ArrayList<>();

    public IntegratedDataDeserializer(boolean lazy) {
        supportedDeserializers.add((int) AvroDataSerDeConstants.MAGIC, new AvroDataDeserializer(lazy));
    }

    public IntegratedDataDeserializer setSchemaCache(SchemaCache cache) {
        for (DataDeserializer dataDeserializer : supportedDeserializers) {
            dataDeserializer.setSchemaCache(cache);
        }
        return this;
    }

    @Override
    public SchemaAndValue deserialize(ByteBuffer byteBuffer) throws SchemaNotFoundException {
        int magic;
        magic = byteBuffer.get(0);
        DataDeserializer deserializer = supportedDeserializers.get(magic);
        if (deserializer != null) {
            return deserializer.deserialize(byteBuffer);
        } else {
            throw new IllegalStateException("Invalid record magic [" + magic + "]");
        }
    }
}
