package com.aliyun.dts.subscribe.clients.record.value;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class  BitValue implements Value<ByteBuffer> {
    private ByteBuffer value;

    public BitValue() {
    }

    public BitValue(byte[] value) {
        this.value = ByteBuffer.wrap(value);
    }

    public BitValue(ByteBuffer value) {
        this.value = value;
    }

    @Override
    public ValueType getType() {
        return ValueType.BIT;
    }

    @Override
    public ByteBuffer getData() {
        return value;
    }

    @Override
    public String toString() {
        try {
            return new String(value.array(), "utf-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long size() {
        if (null != value) {
            return value.capacity();
        }

        return 0L;
    }
}
