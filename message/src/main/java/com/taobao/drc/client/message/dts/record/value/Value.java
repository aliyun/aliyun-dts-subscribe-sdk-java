package com.taobao.drc.client.message.dts.record.value;

public interface Value<T> {

    /**
     * 获取Value定义类型
     */
    ValueType getType();

    /**
     * Get the internal data of current value.
     */
    T getData();

    /**
     * Convert current to string by utf-8 encoding.
     */
    String toString();

    /**
     * Get the size of current value.
     */
    long size();

    /**
     * Parse to a new object according to @rawData.
     * @param rawData the data that presents Value<T>, which can only be recognized by this Value type.
     */
    Value<T> parse(Object rawData);
}
