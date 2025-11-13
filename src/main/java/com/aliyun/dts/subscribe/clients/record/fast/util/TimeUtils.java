package com.aliyun.dts.subscribe.clients.record.fast.util;

public class TimeUtils {
    public static long getTimestampSeconds(long ts) {
        return ts / 10000000000L > 0 ? ts / 1000 : ts;
    }
}
