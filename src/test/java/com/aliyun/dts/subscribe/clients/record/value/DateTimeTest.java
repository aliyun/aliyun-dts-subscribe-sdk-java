package com.aliyun.dts.subscribe.clients.record.value;

import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

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
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.35", DateTime.SEG_DATETIME_NAONS).toString(), "2022-02-15 16:16:56.35");
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.035", DateTime.SEG_DATETIME_NAONS).toString(), "2022-02-15 16:16:56.035");
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.35", DateTime.SEG_DATETIME_NAONS).toString(), "2022-02-15 16:16:56.35");
        Assert.assertEquals(new DateTime("2022-02-15 16:16:56.350", DateTime.SEG_DATETIME_NAONS).toString(), "2022-02-15 16:16:56.35");
    }

    @Test
    public void testDate() throws ParseException {
        DateTime dateTime = new DateTime("2022-02-15 16:16:56.350", DateTime.SEG_DATETIME_NAONS);

        System.out.println(dateTime);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS");


        System.out.println(formatter.parse(dateTime.toString()).getTime());

    }
}
