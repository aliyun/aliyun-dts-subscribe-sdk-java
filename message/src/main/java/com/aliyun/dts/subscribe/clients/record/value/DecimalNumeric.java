package com.aliyun.dts.subscribe.clients.record.value;


import java.math.BigDecimal;

public class DecimalNumeric implements Value<BigDecimal> {

    private BigDecimal data;

    public DecimalNumeric() {
    }

    public DecimalNumeric(BigDecimal data) {
        this.data = data;
    }

    public DecimalNumeric(String data) {
        if (null == data) {
            return;
        }
        this.data = new BigDecimal(data);
    }

    @Override
    public ValueType getType() {
        return ValueType.DECIMAL_NUMERIC;
    }

    @Override
    public BigDecimal getData() {
        return this.data;
    }

    @Override
    public String toString() {
        if (null == this.data) {
            return null;
        }
        return this.data.toString();
    }

    @Override
    public long size() {
        if (null != data) {
            return data.toBigInteger().toByteArray().length;
        }
        return 0L;
    }
}
