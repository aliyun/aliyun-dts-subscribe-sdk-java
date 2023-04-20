package com.taobao.drc.togo.data;

import com.taobao.drc.togo.data.schema.SchemaMetaData;

/**
 * @author yangyang
 * @since 17/3/8
 */
public interface DataSerializer {
    byte[] serialize(Object object, SchemaMetaData schemaMetaData);
}
