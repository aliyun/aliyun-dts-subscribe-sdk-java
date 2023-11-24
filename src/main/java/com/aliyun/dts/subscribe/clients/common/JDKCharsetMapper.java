package com.aliyun.dts.subscribe.clients.common;

import java.util.HashMap;
import java.util.Map;

public interface JDKCharsetMapper {

    Map<String, String> MYSQL_JDK_ENCODINGS = new HashMap<String, String>() {
        {
            put("armscii8", "WINDOWS-1252");
            put("ascii", "US-ASCII");
            put("big5", "BIG5");
            put("binary", "ISO-8859-1");
            put("cp1250", "Cp1250");
            put("cp1251", "Cp1251");
            put("cp1256", "WINDOWS-1256");
            put("cp1257", "Cp1257");
            put("cp850", "IBM850");
            put("cp852", "IBM852");
            put("cp866", "Cp866");
            put("cp932", "Cp932");
            put("dec8", "WINDOWS-1252");
            put("eucjpms", "X-EUCJP-OPEN");
            put("euckr", "EUC_KR");
            put("gb2312", "EUC_CN");
            put("gbk", "GBK");
            put("geostd8", "WINDOWS-1252");
            put("greek", "ISO8859_7");
            put("hebrew", "ISO8859_8");
            put("hp8", "WINDOWS-1252");
            put("keybcs2", "IBM852");
            put("koi8r", "KOI8-R");
            put("koi8u", "KOI8-R");
            put("latin1", "Cp1252");
            put("latin2", "ISO8859_2");
            put("latin5", "ISO-8859-9");
            put("latin7", "ISO-8859-13");
            put("macce", "MacCentralEurope");
            put("macroman", "MacRoman");
            put("sjis", "SJIS");
            put("swe7", "WINDOWS-1252");
            put("tis620", "TIS620");
            put("ujis", "EUC_JP");
            put("utf16", "UTF-16");
            put("utf16le", "UTF-16LE");
            put("utf32", "UTF-32");
            put("utf8", "UTF-8");
            put("utf8mb4", "UTF-8");
            put("utf8mb3", "UTF-8");
            put("ucs2", "UnicodeBig");
        }
    };

    static String getJDKECharset(String dbCharset) {
        return MYSQL_JDK_ENCODINGS.getOrDefault(dbCharset, dbCharset);
    }
}
