package com.aliyun.dts.subscribe.clients.record.value;

public interface Value<T> {
    /**
     * @return get value type
     */
    ValueType getType();

    /**
     * @return Get the internal data of current value.
     */
    T getData();

    /**
     * @return Convert current to string by utf-8 encoding.
     */
    String toString();

    /**
     * @return Get the size of current value.
     */
    long size();
}
