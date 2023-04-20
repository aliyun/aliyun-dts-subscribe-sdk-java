package com.taobao.drc.togo.data.schema;

/**
 * @author yangyang
 * @since 17/3/8
 */
public interface SchemaMetaData {
    String topic();

    String name();

    Schema schema();

    Integer id();

    Integer version();
}
