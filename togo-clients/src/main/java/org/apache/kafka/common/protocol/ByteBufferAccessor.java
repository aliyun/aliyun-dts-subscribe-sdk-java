package org.apache.kafka.common.protocol;


import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;

public class ByteBufferAccessor implements Readable, Writable {
    private final ByteBuffer buf;

    public ByteBufferAccessor(ByteBuffer buf) {
        this.buf = buf;
    }

    @Override
    public byte readByte() {
        return buf.get();
    }

    @Override
    public short readShort() {
        return buf.getShort();
    }

    @Override
    public int readInt() {
        return buf.getInt();
    }

    @Override
    public long readLong() {
        return buf.getLong();
    }

    @Override
    public void readArray(byte[] arr) {
        buf.get(arr);
    }

    @Override
    public int readUnsignedVarint() {
        return ByteUtils.readUnsignedVarint(buf);
    }

    @Override
    public void writeByte(byte val) {
        buf.put(val);
    }

    @Override
    public void writeShort(short val) {
        buf.putShort(val);
    }

    @Override
    public void writeInt(int val) {
        buf.putInt(val);
    }

    @Override
    public void writeLong(long val) {
        buf.putLong(val);
    }

    @Override
    public void writeByteArray(byte[] arr) {
        buf.put(arr);
    }

    @Override
    public void writeUnsignedVarint(int i) {
        ByteUtils.writeUnsignedVarint(i, buf);
    }
}
