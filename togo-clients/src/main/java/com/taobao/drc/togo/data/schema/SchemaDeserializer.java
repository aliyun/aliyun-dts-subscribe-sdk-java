package com.taobao.drc.togo.data.schema;

import java.nio.ByteBuffer;

/**
 * @author yangyang
 * @since 17/3/15
 */
public interface SchemaDeserializer {
    Schema deserialize(ByteBuffer buffer);
}
