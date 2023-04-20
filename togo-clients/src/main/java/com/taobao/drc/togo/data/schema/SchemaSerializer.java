package com.taobao.drc.togo.data.schema;

/**
 * @author yangyang
 * @since 17/3/15
 */
public interface SchemaSerializer {
    byte[] serialize(Schema schema);
}
