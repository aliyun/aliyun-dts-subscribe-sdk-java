package com.aliyun.dts.subscribe.clients.record.value;

import com.aliyun.dts.subscribe.clients.common.BytesUtil;
import com.aliyun.dts.subscribe.clients.common.JDKCharsetMapper;
import com.aliyun.dts.subscribe.clients.common.function.SwallowException;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class StringValue implements Value<ByteBuffer> {

    public static final String DEFAULT_CHARSET = "UTF-8";
    private ByteBuffer data;
    private String charset;

    public StringValue(ByteBuffer data, String charset) {
        this.data = data;
        this.charset = charset;
    }

    public StringValue(String data) {
        this(ByteBuffer.wrap(
                SwallowException.callAndThrowRuntimeException(() -> data.getBytes(DEFAULT_CHARSET))),
                DEFAULT_CHARSET);
    }

    public String getCharset() {
        return this.charset;
    }

    @Override
    public ValueType getType() {
        return ValueType.STRING;
    }

    @Override
    public ByteBuffer getData() {
        return this.data;
    }

    @Override
    public String toString() {

        // just return hex string if missing charset
        if (StringUtils.isEmpty(charset)) {
            return BytesUtil.byteBufferToHexString(data);
        }

        // try encode data by specified charset
        try {
            if (!StringUtils.isEmpty(charset)) {
                return new String(data.array(), charset);
            }
            return new String(data.array());
        } catch (UnsupportedEncodingException e1) {
            try {
                return new String(data.array(), JDKCharsetMapper.getJDKECharset(charset));
            } catch (UnsupportedEncodingException e2) {
                return charset + "_'" + BytesUtil.byteBufferToHexString(data) + "'";
            }
        }
    }

    public String toString(String targetCharset) {
        //TODO(huoyu): convert
        return "to impl";
    }

    @Override
    public long size() {
        if (null != data) {
            return data.capacity();
        }

        return 0L;
    }
}
