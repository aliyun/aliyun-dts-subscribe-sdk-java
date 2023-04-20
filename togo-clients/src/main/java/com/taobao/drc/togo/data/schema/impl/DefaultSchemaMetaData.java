package com.taobao.drc.togo.data.schema.impl;

import com.taobao.drc.togo.data.schema.Schema;
import com.taobao.drc.togo.data.schema.SchemaMetaData;

/**
 * @author yangyang
 * @since 17/3/22
 */
public class DefaultSchemaMetaData implements SchemaMetaData {
    private final String topic;
    private final String name;
    private final Schema schema;
    private final Integer id;
    private final Integer version;

    public DefaultSchemaMetaData(String topic, String name, Schema schema, Integer id, Integer version) {
        this.topic = topic;
        this.name = name;
        this.schema = schema;
        this.id = id;
        this.version = version;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Integer id() {
        return id;
    }

    @Override
    public Integer version() {
        return version;
    }

    @Override
    public String toString() {
        return "DefaultSchemaMetaData{" +
                "topic='" + topic + '\'' +
                ", name='" + name + '\'' +
                ", schema=" + schema +
                ", id=" + id +
                ", version=" + version +
                '}';
    }
}
