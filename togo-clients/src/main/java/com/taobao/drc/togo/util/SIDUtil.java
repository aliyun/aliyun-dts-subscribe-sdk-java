package com.taobao.drc.togo.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Base64;

public class SIDUtil {
    public static final String SIDSplitChar = "-";
    public static final String BASE64_SPLIT = "&";
    public static final String JAAS_INTERCEP_TAG = "dts.api.url";

    public static String extractSIDFromPlainDescribe(String data) {
        return StringUtils.substringBefore(data, SIDSplitChar);
    }

    public static String getUserNameFromString(String data) {
        return getItemFromString(data, 0);
    }

    public static String getSIDFromString(String data) {
        return getItemFromString(data, 1);
    }

    public static String getPasswordFromString(String data) {
        return getItemFromString(data, 2);
    }

    public static String encode(String userName, String sid, String password) {
        return Base64.encoder().encodeToString((userName + BASE64_SPLIT + sid + BASE64_SPLIT + password).getBytes());
    }

    public static String encode(String userName, String sid, String password, String ip, int port) {
        return Base64.encoder().encodeToString((userName + BASE64_SPLIT + sid + BASE64_SPLIT + password + BASE64_SPLIT + ip + BASE64_SPLIT+port).getBytes());
    }

    private static String getItemFromString(String data, int index) {
        if (data==null || data.isEmpty()) {
            throw new KafkaException("get uid failed cause input is empty");
        }
        String decoded =  new String(Base64.decoder().decode(data));
        String[] UIDAndNameSplitArray =decoded.split(BASE64_SPLIT);
        if (UIDAndNameSplitArray.length < 3) {
            throw new KafkaException("invalid uid format " + data);
        }
        return UIDAndNameSplitArray[index];
    }
}
