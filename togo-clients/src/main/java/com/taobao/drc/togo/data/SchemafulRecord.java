package com.taobao.drc.togo.data;

/**
 * @author yangyang
 * @since 17/3/8
 */
public interface SchemafulRecord extends SchemaContainer {
    Object get(String key);

    SchemafulRecord put(String key, Object t);

    Object get(int index);

    SchemafulRecord put(int index, Object t);
}
