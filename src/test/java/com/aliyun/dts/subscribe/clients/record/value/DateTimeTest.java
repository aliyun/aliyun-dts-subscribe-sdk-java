package com.aliyun.dts.subscribe.clients.record.value;

import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;

public class DateTimeTest {
    @Test
    public void testToUnixTimestamp() throws ParseException {
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.35", DateTime.SEG_DATETIME_NAONS).toUnixTimestamp(), 1644913016350L);
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.035", DateTime.SEG_DATETIME_NAONS).toUnixTimestamp(), 1644913016035L);
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.35", DateTime.SEG_DATETIME_NAONS).toUnixTimestamp(), 1644913016350L);
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.350", DateTime.SEG_DATETIME_NAONS).toUnixTimestamp(), 1644913016350L);
    }

    @Test
    public void testToEpochMilliSeconds() throws ParseException {
        System.out.println(new DateTime("2022-02-15 16:16:56.35", DateTime.SEG_DATETIME_NAONS).toEpochMilliSeconds());
        System.out.println(new DateTime("2022-02-15 16:16:56.035", DateTime.SEG_DATETIME_NAONS).toEpochMilliSeconds());
        System.out.println(new DateTime("2022-02-15 16:16:56.35", DateTime.SEG_DATETIME_NAONS).toEpochMilliSeconds());
        System.out.println(new DateTime("2022-02-15 16:16:56.350", DateTime.SEG_DATETIME_NAONS).toEpochMilliSeconds());

        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.35", DateTime.SEG_DATETIME_NAONS).toEpochMilliSeconds(), 1644913016350L);
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.035", DateTime.SEG_DATETIME_NAONS).toEpochMilliSeconds(), 1644913016035L);
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.35", DateTime.SEG_DATETIME_NAONS).toEpochMilliSeconds(), 1644913016350L);
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.350", DateTime.SEG_DATETIME_NAONS).toEpochMilliSeconds(), 1644913016350L);

    }

    @Test
    public void testToString() {
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.35", DateTime.SEG_DATETIME_NAONS), "2022-02-15 16:16:56.35");
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.035", DateTime.SEG_DATETIME_NAONS), "2022-02-15 16:16:56.035");
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.35", DateTime.SEG_DATETIME_NAONS), "2022-02-15 16:16:56.35");
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.350", DateTime.SEG_DATETIME_NAONS), "2022-02-15 16:16:56.35");
    }
}
