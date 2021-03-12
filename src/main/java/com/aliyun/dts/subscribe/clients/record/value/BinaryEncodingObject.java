package com.aliyun.dts.subscribe.clients.record.value;

import com.aliyun.dts.subscribe.clients.common.BytesUtil;

import java.nio.ByteBuffer;

public class BinaryEncodingObject implements Value<ByteBuffer> {

    private ObjectType objectType;
    private ByteBuffer binaryData;

    public BinaryEncodingObject(ObjectType objectType, ByteBuffer binaryData) {
        this.objectType = objectType;
        this.binaryData = binaryData;
    }

    @Override
    public ValueType getType() {
        return ValueType.BINARY_ENCODING_OBJECT;
    }

    @Override
    public ByteBuffer getData() {
        return binaryData;
    }

    public ObjectType getObjectType() {
        return this.objectType;
    }

    @Override
    public long size() {
        if (null != binaryData) {
            return binaryData.capacity();
        }

        return 0L;
    }

    public String toString() {
        return BytesUtil.byteBufferToHexString(binaryData);
    }
}
