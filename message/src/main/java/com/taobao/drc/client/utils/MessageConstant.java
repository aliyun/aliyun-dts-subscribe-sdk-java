package com.taobao.drc.client.utils;

import java.nio.charset.Charset;

public class MessageConstant {
    public static final String DEFAULT_ENCODING = "UTF8";
    public static final String DEFAULT_FIELD_ENCODING = "ASCII";

    public static final Charset DEFAULT_CHARSET = Charset.forName(DEFAULT_ENCODING);


    public static final byte special_n = '\n';

    public static final byte kv_split = ':';
}
