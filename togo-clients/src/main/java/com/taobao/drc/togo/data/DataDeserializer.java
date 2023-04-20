package com.taobao.drc.togo.data;

import com.taobao.drc.togo.data.schema.SchemaCache;

import java.nio.ByteBuffer;

/**
 * @author yangyang
 * @since 17/3/8
 */
public interface DataDeserializer {
    DataDeserializer setSchemaCache(SchemaCache cache);

    SchemaAndValue deserialize(ByteBuffer byteBuffer) throws SchemaNotFoundException;
}
