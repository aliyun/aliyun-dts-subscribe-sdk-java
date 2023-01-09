package com.taobao.drc.client.message;

import com.taobao.drc.client.utils.MessageConstant;

import java.io.UnsupportedEncodingException;

public class ByteString {

    private int len;

    private int offset;

    private byte[] bytes;


    public ByteString(byte[] bytes) {
        this(bytes, bytes.length);
    }

    public ByteString(byte[] bytes, int len) {
        this.bytes = bytes;
        this.len = len;
    }

    public ByteString(byte[] bytes, int offset, int len) {
        this.bytes = bytes;
        this.len = len;
        this.offset = offset;
    }

    /**
     * Convert the bytes to any encoding.
     *
     * @param encoding the target encoding.
     * @return the encoded string.
     * @throws UnsupportedEncodingException
     */
    public String toString(final String encoding)
            throws UnsupportedEncodingException {

        if (len == 0)
            return new String("");

        if (encoding.equalsIgnoreCase("binary"))
            throw new UnsupportedEncodingException
                    ("field encoding: binary, use getBytes() instead of toString()");

        String realEncoding = encoding;
        if (encoding.isEmpty() || encoding.equalsIgnoreCase("null"))
            realEncoding = "ASCII";
        else if (encoding.equalsIgnoreCase("utf8mb4"))
            realEncoding = "utf8";
        else if (encoding.equalsIgnoreCase("latin1"))
            realEncoding = "cp1252";
        else if (encoding.equalsIgnoreCase("latin2"))
            realEncoding = "iso-8859-2";
        return new String(bytes, offset, len, realEncoding);
    }

    public String toString() {
        if (len == 0)
            return "";
        return new String(bytes, offset, len, MessageConstant.DEFAULT_CHARSET);
    }

    public byte[] getBytes() {
        byte t[] = new byte[len];
        System.arraycopy(bytes, offset, t, 0, len);
        return t;
    }

    public int getLen() {
        return len;
    }

    public int getOffset() {
        return offset;
    }

}
