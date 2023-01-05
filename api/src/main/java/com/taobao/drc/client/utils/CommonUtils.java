package com.taobao.drc.client.utils;

import com.taobao.drc.client.DRCClientException;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.enums.DBType;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jianjundeng on 3/3/15.
 */
public class CommonUtils {

    public static final String TOPIC_MATCH_REGEX = "^\\w+\\-[0-9]+\\-[0-9]+$";

    public static int byteToInt(byte[] data) {
        int p = 0;
        for (byte c : data) {
            if (c >= 48 && c <= 57) {
                c -= 48;
                p *= 10;
                p += c;
            }
        }
        return p;
    }

    public static boolean isValidTopicName(String topicName) {
        Pattern p = Pattern.compile(TOPIC_MATCH_REGEX);
        Matcher m = p.matcher(topicName);
        return m.matches();
    }

    public static DBType getDatabaseType(String type) {
        if (null == type) {
            return DBType.UNKNOWN;
        }
        if (type.equalsIgnoreCase("mysql"))
            return DBType.MYSQL;
        else if (type.equalsIgnoreCase("Oceanbase") ||
                type.equalsIgnoreCase("ob05"))
            return DBType.OCEANBASE;
        else if (type.equalsIgnoreCase("oceanbase1") ||
                type.equalsIgnoreCase("OB10"))
            return DBType.OCEANBASE1;
        else
            return DBType.UNKNOWN;
    }
}
