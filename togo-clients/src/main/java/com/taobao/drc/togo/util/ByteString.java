package com.taobao.drc.togo.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * @author yangyang
 * @since 17/5/5
 */
public class ByteString {
    private final byte[] bytes;
    private final int offset;
    private final int length;
    private final Charset charset;

    public ByteString(String str) {
        this(str.getBytes(Charsets.UTF_8));
    }

    public ByteString(ByteBuffer buffer) {
        this(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining(), Charsets.UTF_8);
    }

    public ByteString(byte[] bytes) {
        this(bytes, 0, bytes.length, Charsets.UTF_8);
    }

    public ByteString(byte[] bytes, int offset, int length, Charset charset) {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        this.charset = charset;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ByteString that = (ByteString) o;
        if (length != that.length) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (bytes[offset + i] != that.bytes[that.offset + i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (int i = offset; i < offset + length; i++) {
            result = 31 * result + bytes[i];
        }
        return result;
    }

    @Override
    public String toString() {
        return new String(bytes, offset, length, charset);
    }
}
