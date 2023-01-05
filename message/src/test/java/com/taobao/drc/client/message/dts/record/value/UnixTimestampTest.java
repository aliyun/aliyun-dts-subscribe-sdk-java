package com.taobao.drc.client.message.dts.record.value;

import org.junit.Test;

public class UnixTimestampTest {
    @Test
    public void testDatetimeResetSegments() {
        UnixTimestamp timestamp = new UnixTimestamp(1627553369L, 8);
        System.out.println("timestamp: " + timestamp.toString());
    }
}
