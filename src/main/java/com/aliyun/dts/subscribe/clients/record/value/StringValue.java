package com.aliyun.dts.subscribe.clients.record.value;

import com.aliyun.dts.subscribe.clients.common.BytesUtil;
import com.aliyun.dts.subscribe.clients.common.JDKCharsetMapper;
import com.aliyun.dts.subscribe.clients.common.function.SwallowException;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class StringValue implements Value<ByteBuffer> {

    public static final String DEFAULT_CHARSET = "UTF-8";
    private ByteBuffer data;
    private String charset;
    private String rawString;
    private transient String toStringCache;
    private static ThreadLocal<Map<String, Charset>> charsetMap = new ThreadLocal<>();

    public StringValue(ByteBuffer data, String charset) {
        this.data = data;
        this.charset = charset;
    }

    public StringValue(String data) {
        this.rawString = data;
        this.charset = DEFAULT_CHARSET;
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

        if (rawString != null) {
            return rawString;
        }
        if (data == null) {
            return null;
        }
        if (null != toStringCache) {
            return toStringCache;
        }
        // just return hex string if missing charset
        if (StringUtils.isEmpty(charset)) {
            return BytesUtil.byteBufferToHexString(BytesUtil.newInitialByteBuffer(data));
        }

        // try encode data by specified charset
        Map<String, Charset> localMap = charsetMap.get();
        if (null == localMap) {
            localMap = new HashMap<>();
            charsetMap.set(localMap);
        }
        try {
            Charset charsetObject = localMap.computeIfAbsent(charset, key -> Charset.forName(charset));
            toStringCache = new String(data.array(), data.arrayOffset(), data.remaining(), charsetObject);
        } catch (Exception e1) {
            try {
                Charset charsetObject = localMap.computeIfAbsent(charset, key -> Charset.forName(JDKCharsetMapper.getJDKECharset(charset)));
                toStringCache = new String(data.array(), data.arrayOffset(), data.remaining(), charsetObject);
            } catch (Exception e2) {
                toStringCache = charset + "_'" + BytesUtil.byteBufferToHexString(data) + "'";
            }
        }
        return toStringCache;
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
