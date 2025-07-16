package com.aliyun.dts.subscribe.clients.record.fast.util;

import com.aliyun.dts.subscribe.clients.record.UserRecord;

import java.util.Map;

public class RecordTools {
    public static final String META_VERSION_KEY = "metaVersion";

    public static long getMetaVersion(UserRecord record) {
        String rs = getTagValue(record, META_VERSION_KEY);
        if (null == rs) {
            return 0;
        }
        return Long.parseLong(rs);
    }

    public static String getTagValue(UserRecord record, String tagName) {
        if (null != record) {
            final Map<String, String> property = record.getExtendedProperty();
            if (null != property) {
                return property.getOrDefault(tagName, null);
            }
        }
        return null;
    }
}
