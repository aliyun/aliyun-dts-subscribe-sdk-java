package com.aliyun.dts.subscribe.clients.record.value;

import org.junit.Assert;
import org.junit.Test;

public class TimestampTest {
    @Test
    public void UnixTimestampTest() {

        UnixTimestamp timestamp = new UnixTimestamp(1627553369L, 0);
        System.out.println("timestamp: " + timestamp.toString());
        Assert.assertTrue("2021-07-29 18:09:29".equals(timestamp.toString()));

        UnixTimestamp timestamp2 = new UnixTimestamp(1627553369L, 8);
        System.out.println("timestamp2: " + timestamp2.toString());
        Assert.assertTrue("2021-07-29 18:09:29.000008".equals(timestamp2.toString()));
    }
}
