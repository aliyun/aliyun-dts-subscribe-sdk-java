package com.taobao.drc.togo.data.schema;

import com.taobao.drc.togo.util.concurrent.TogoFuture;

import java.util.List;

/**
 * @author yangyang
 * @since 17/3/8
 */
public interface SchemaRegistry {
    TogoFuture<SchemaMetaData> register(String topic, String name, Schema schema);

    TogoFuture<SchemaMetaData> get(String topic, int schemaId, int version);

    TogoFuture<Integer> getSchemaIdByName(String topic, String name);

    TogoFuture<List<SchemaMetaData>> getSchemaListByName(String topic, String name);
}
