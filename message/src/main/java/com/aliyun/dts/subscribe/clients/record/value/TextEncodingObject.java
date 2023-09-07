package com.aliyun.dts.subscribe.clients.record.value;

import org.apache.commons.lang3.StringUtils;

public class TextEncodingObject implements Value<String> {

    private ObjectType objectType;
    private String data;

    public TextEncodingObject(ObjectType objectType, String data) {
        this.objectType = objectType;
        this.data = data;
    }

    @Override
    public ValueType getType() {
        return ValueType.TEXT_ENCODING_OBJECT;
    }

    @Override
    public String getData() {
        return this.data;
    }

    @Override
    public long size() {
        return StringUtils.length(data);
    }

    public ObjectType getObjectType() {
        return objectType;
    }

    public String toString() {
        return  data;
    }
}
