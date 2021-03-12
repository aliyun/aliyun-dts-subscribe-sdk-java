package com.aliyun.dts.subscribe.clients.record.value;


import java.math.BigInteger;

public class IntegerNumeric implements Value {

    private BigInteger data;

    public IntegerNumeric() {
    }

    public IntegerNumeric(long value) {
        data = BigInteger.valueOf(value);
    }

    public IntegerNumeric(BigInteger value) {
        this.data = value;
    }

    public IntegerNumeric(String value) {
        this.data = new BigInteger(value);
    }

    @Override
    public ValueType getType() {
        return ValueType.INTEGER_NUMERIC;
    }

    public BigInteger getData() {
        return this.data;
    }

    @Override
    public String toString() {
        return this.data.toString();
    }

    @Override
    public long size() {
        if (null != data) {
            return data.toByteArray().length;
        }

        return 0L;
    }

    public IntegerNumeric parse(Object rawData) {
        if (null == rawData) {
            return null;
        }

        return new IntegerNumeric(rawData.toString());
    }
}
