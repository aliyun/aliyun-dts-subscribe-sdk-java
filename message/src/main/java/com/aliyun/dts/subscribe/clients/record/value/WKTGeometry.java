package com.aliyun.dts.subscribe.clients.record.value;

import org.apache.commons.lang3.StringUtils;

public class WKTGeometry implements Value<String> {

    private long srid;
    private String data;

    public WKTGeometry(String data) {
        this.data = data;
    }

    @Override
    public ValueType getType() {
        return ValueType.WKT_GEOMETRY;
    }

    @Override
    public String getData() {
        return this.data;
    }

    @Override
    public long size() {
        if (null != data) {
            return StringUtils.length(data);
        }

        return 0L;
    }

    public String toString() {
        return data;
    }

    public WKTGeometry parse(Object rawData) {
        if (null == rawData) {
            return null;
        }

        return new WKTGeometry(rawData.toString());
    }
}
