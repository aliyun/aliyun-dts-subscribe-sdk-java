package com.taobao.drc.client.message.dts.record.value;

import com.taobao.drc.client.message.dts.record.utils.BytesUtil;

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

    @Override
    public String toString() {
        return BytesUtil.byteBufferToHexString(binaryData);
    }

    @Override
    public BinaryEncodingObject parse(Object rawData) {
        if (null == rawData) {
            return null;
        }

        ByteBuffer byteBuffer = null;

        if (rawData instanceof String) {
            byteBuffer = BytesUtil.hexStringToByteBuffer(rawData.toString());
        } else if (rawData instanceof byte[]) {
            byteBuffer = ByteBuffer.wrap((byte[]) rawData);
        }

        return new BinaryEncodingObject(objectType, byteBuffer);
    }
}
