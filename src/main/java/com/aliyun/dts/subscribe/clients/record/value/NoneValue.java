package com.aliyun.dts.subscribe.clients.record.value;

/**
 * 占位字段,不具有任何意义
 */
public class NoneValue implements Value<Boolean> {

    @Override
    public ValueType getType() {
        return ValueType.NONE;
    }

    @Override
    public Boolean getData() {
        return false;
    }

    @Override
    public long size() {
        return 0L;
    }
}