package com.taobao.drc.togo.data.schema;

/**
 * @author yangyang
 * @since 17/3/16
 */
public interface SchemaConverter<T> {
    T toImpl(Schema schema);

    Schema fromImpl(T t);
}
