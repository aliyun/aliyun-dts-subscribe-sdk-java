package com.taobao.drc.client.message.dts.record.value;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.*;

public class DateTimeTest {

    @Test
    public void testDatetimeResetSegments() {
        DateTime normalValue = new DateTime("2018-01-29 11:59:10.0 Asia/Shanghai", DateTime.SEG_DATETIME_NAONS_TZ);
        Assert.assertTrue(StringUtils.equals(normalValue.toJdbcString(DateTime.SEG_DATETIME_NAONS), "2018-01-29 11:59:10.0"));
        Assert.assertTrue(StringUtils.equals(normalValue.getTimeZone(), "Asia/Shanghai"));
    }

    @Test
    public void testDatetimeResetSegments2() {
        DateTime normalValue = new DateTime("5709-03-27 10:57:06.9177230 -02:28", DateTime.SEG_DATETIME_NAONS_TZ);
        Assert.assertEquals("5709-03-27 10:57:06.917723", normalValue.toJdbcString(DateTime.SEG_DATETIME_NAONS));
        Assert.assertEquals(normalValue.getTimeZone(), "-02:28");

        DateTime normalValue2 = new DateTime("10:57:06.9177230 -02:28", DateTime.SEG_TIME_NAONS | DateTime.SEG_TIMEZONE);
        Assert.assertEquals("10:57:06.917723 -02:28", normalValue2.toJdbcString(DateTime.SEG_TIME_NAONS | DateTime.SEG_TIMEZONE));
        Assert.assertEquals("-02:28", normalValue2.getTimeZone());
    }

    @Test
    public void testDatetimeDatetimeTest1() {
        DateTime dateTime = new DateTime();
        dateTime.setSegments(DateTime.SEG_DATETIME);

        dateTime.setYear(2019);
        dateTime.setMonth(9);
        dateTime.setDay(11);
        dateTime.setHour(12);
        dateTime.setMinute(13);
        dateTime.setSecond(14);
        assertEquals("2019-09-11 12:13:14", dateTime.toString());

        dateTime.setYear(0);
        dateTime.setMonth(0);
        dateTime.setDay(0);
        dateTime.setHour(0);
        dateTime.setMinute(0);
        dateTime.setSecond(0);
        assertEquals("0000-00-00 00:00:00", dateTime.toString());

        dateTime.setYear(1);
        dateTime.setMonth(1);
        dateTime.setDay(1);
        dateTime.setHour(1);
        dateTime.setMinute(1);
        dateTime.setSecond(1);
        assertEquals("0001-01-01 01:01:01", dateTime.toString());

        assertEquals("0001-01-01 01:01:01", dateTime.getData());
    }

    @Test
    public void testDatetimeDatetimeNaonsTest1() {
        DateTime dateTime = new DateTime();
        dateTime.setSegments(DateTime.SEG_DATETIME_NAONS);

        dateTime.setYear(2019);
        dateTime.setMonth(9);
        dateTime.setDay(11);
        dateTime.setHour(12);
        dateTime.setMinute(13);
        dateTime.setSecond(14);
        dateTime.setNaons(12345);
        assertEquals("2019-09-11 12:13:14.000012345", dateTime.toString());

        dateTime.setYear(0);
        dateTime.setMonth(0);
        dateTime.setDay(0);
        dateTime.setHour(0);
        dateTime.setMinute(0);
        dateTime.setSecond(0);
        dateTime.setNaons(1);
        assertEquals("0000-00-00 00:00:00.000000001", dateTime.toString());

        dateTime.setYear(1);
        dateTime.setMonth(1);
        dateTime.setDay(1);
        dateTime.setHour(1);
        dateTime.setMinute(1);
        dateTime.setSecond(1);
        dateTime.setNaons(10);
        assertEquals("0001-01-01 01:01:01.00000001", dateTime.toString());

        assertEquals("0001-01-01 01:01:01.00000001", dateTime.getData());
    }

    @Test
    public void testDatetimeTimeTest1() {
        DateTime dateTime = new DateTime();
        dateTime.setSegments(DateTime.SEG_TIME);

        dateTime.setHour(12);
        dateTime.setMinute(13);
        dateTime.setSecond(14);
        dateTime.setNaons(12345);
        assertEquals("12:13:14", dateTime.toString());

        dateTime.setHour(0);
        dateTime.setMinute(0);
        dateTime.setSecond(0);
        dateTime.setNaons(1);
        assertEquals("00:00:00", dateTime.toString());

        dateTime.setHour(1);
        dateTime.setMinute(1);
        dateTime.setSecond(1);
        dateTime.setNaons(10);
        assertEquals("01:01:01", dateTime.toString());

        assertEquals("01:01:01", dateTime.getData());
    }

    @Test
    public void testDatetimeTimeNaonsTest1() {
        DateTime dateTime = new DateTime();
        dateTime.setSegments(DateTime.SEG_TIME | DateTime.SEG_NAONS);

        dateTime.setHour(12);
        dateTime.setMinute(13);
        dateTime.setSecond(14);
        dateTime.setNaons(12345000);
        assertEquals("12:13:14.012345", dateTime.toString());

        dateTime.setHour(0);
        dateTime.setMinute(0);
        dateTime.setSecond(0);
        dateTime.setNaons(1000);
        assertEquals("00:00:00.000001", dateTime.toString());

        dateTime.setHour(1);
        dateTime.setMinute(1);
        dateTime.setSecond(1);
        dateTime.setNaons(10000);
        assertEquals("01:01:01.00001", dateTime.toString());

        dateTime.setHour(2);
        dateTime.setMinute(2);
        dateTime.setSecond(2);
        dateTime.setNaons(0);
        assertEquals("02:02:02.0", dateTime.toString());

        dateTime.setHour(-838);
        dateTime.setMinute(59);
        dateTime.setSecond(59);
        dateTime.setNaons(590000000);
        dateTime.setSegments(dateTime.getSegments());
        assertEquals("-838:59:59.59", dateTime.toString());
        assertTrue(dateTime.isNegative());

        dateTime.clearSegments(DateTime.SEG_NEGATIVE);
        dateTime.setHour(838);
        dateTime.setMinute(59);
        dateTime.setSecond(59);
        dateTime.setNaons(590000000);
        dateTime.setSegments(dateTime.getSegments());
        assertEquals("838:59:59.59", dateTime.toString());
        assertFalse(dateTime.isNegative());
    }

    @Test
    public void testDatetimeTimeMicroTimeZoneTest1() {
        DateTime dateTime = new DateTime();
        dateTime.setSegments(DateTime.SEG_DATETIME_NAONS | DateTime.SEG_TIMEZONE);

        dateTime.setYear(2019);
        dateTime.setMonth(9);
        dateTime.setDay(11);
        dateTime.setHour(12);
        dateTime.setHour(12);
        dateTime.setMinute(13);
        dateTime.setSecond(14);
        dateTime.setNaons(12345000);
        dateTime.setTimeZone("GMT+08:00");
        assertEquals("2019-09-11 12:13:14.012345 GMT+08:00", dateTime.toString());

        dateTime.setYear(0);
        dateTime.setMonth(0);
        dateTime.setDay(0);
        dateTime.setHour(0);
        dateTime.setHour(0);
        dateTime.setMinute(0);
        dateTime.setSecond(0);
        dateTime.setNaons(1000);
        assertEquals("0000-00-00 00:00:00.000001 GMT+08:00", dateTime.toString());

        dateTime.setYear(2019);
        dateTime.setMonth(9);
        dateTime.setDay(11);
        dateTime.setHour(12);
        dateTime.setHour(1);
        dateTime.setMinute(1);
        dateTime.setSecond(1);
        dateTime.setNaons(10000);
        assertEquals("2019-09-11 01:01:01.00001 GMT+08:00", dateTime.toString());

        dateTime.setYear(2019);
        dateTime.setMonth(9);
        dateTime.setDay(11);
        dateTime.setHour(12);
        dateTime.setHour(2);
        dateTime.setMinute(2);
        dateTime.setSecond(2);
        dateTime.setNaons(0);
        assertEquals("2019-09-11 02:02:02.0 GMT+08:00", dateTime.toString());

        assertEquals("2019-09-11 02:02:02.0 GMT+08:00", dateTime.getData());
    }

    @Test
    public void testDatetimeTest1() {

        DateTime dateTime2 = new DateTime("2019-09-11 02:02:02.0123 GMT+08:00", DateTime.SEG_DATETIME_NAONS | DateTime.SEG_TIMEZONE);
        assertEquals("2019-09-11 02:02:02.0123 GMT+08:00", dateTime2.toString());
        assertEquals(12300000, dateTime2.getNaons());

        DateTime dateTime3 = new DateTime("2019-09-11 02:02:02.0123 asia/shanghai", DateTime.SEG_DATETIME_NAONS | DateTime.SEG_TIMEZONE);
        assertEquals("2019-09-11 02:02:02.0123 asia/shanghai", dateTime3.toString());
        System.out.println(dateTime3.toString());
        assertEquals(12300000, dateTime2.getNaons());
    }

    @Test
    public void testDatetimeIntevalLdsTest() {

        DateTime dateTime1 = new DateTime("-09 05:06:07.4", DateTime.SEG_DAY | DateTime.SEG_TIME | DateTime.SEG_NAONS);
        assertEquals("-09 05:06:07.4", dateTime1.toString());

        DateTime dateTime2 = new DateTime("09 05:06:07.4", DateTime.SEG_DAY | DateTime.SEG_TIME | DateTime.SEG_NAONS);
        assertEquals("09 05:06:07.4", dateTime2.toString());

        DateTime dateTime3 = new DateTime();
        dateTime3.setSegments(DateTime.SEG_DAY | DateTime.SEG_TIME | DateTime.SEG_NAONS);
        dateTime3.setDay(-9);
        dateTime3.setHour(1);
        dateTime3.setMinute(2);
        dateTime3.setSecond(3);
        dateTime3.setNaons(4);

        assertEquals("-09 01:02:03.000000004", dateTime3.toString());

        DateTime dateTime4 = new DateTime();
        dateTime4.setSegments(DateTime.SEG_DAY | DateTime.SEG_TIME | DateTime.SEG_NAONS);
        dateTime4.setDay(900000);
        dateTime4.setHour(1);
        dateTime4.setMinute(2);
        dateTime4.setSecond(3);
        dateTime4.setNaons(4);

        assertEquals("900000 01:02:03.000000004", dateTime4.toString());
    }

    @Test
    public void testDatetimeIntevalymTest() {

        DateTime dateTime1 = new DateTime("-200000-11", DateTime.SEG_YEAR | DateTime.SEG_MONTH);
        assertEquals("-200000-11", dateTime1.toString());

        DateTime dateTime2 = new DateTime("200000-11", DateTime.SEG_YEAR | DateTime.SEG_MONTH);
        assertEquals("200000-11", dateTime2.toString());

        DateTime dateTime3 = new DateTime();
        dateTime3.setSegments(DateTime.SEG_YEAR | DateTime.SEG_MONTH);
        dateTime3.setYear(-9);
        dateTime3.setMonth(1000);

        assertEquals("-0009-1000", dateTime3.toString());

        DateTime dateTime4 = new DateTime();
        dateTime4.setSegments(DateTime.SEG_YEAR | DateTime.SEG_MONTH);
        dateTime4.setYear(100000);
        dateTime4.setMonth(2000);

        assertEquals("100000-2000", dateTime4.toString());
    }

    @Test
    public void testToUnixTimestamp() throws ParseException {
        DateTime ti = new DateTime("2018-01-29 11:59:11.123 Asia/Shanghai", DateTime.SEG_DATETIME_NAONS_TZ);
        Assert.assertEquals("2018-01-29 03:59:11.000123", ti.toUnixTimestampValue().toString());
        DateTime t2 = new DateTime("2018-01-29 11:59:11.123", DateTime.SEG_DATETIME_NAONS);
        Assert.assertEquals("2018-01-29 11:59:11.000123", t2.toUnixTimestampValue().toString());
        DateTime t3 = new DateTime("2018-01-29 11:59:11", DateTime.SEG_DATETIME);
        Assert.assertEquals("2018-01-29 11:59:11.0", t3.toUnixTimestampValue().toString());
    }

    @Test
    public void testToUTCZero() throws ParseException {
        DateTime ti = new DateTime("2018-01-29 11:59:11.123 Asia/Shanghai", DateTime.SEG_DATETIME_NAONS_TZ);
//        long datetime = Date.parse(ti.toJdbcString(DateTime.SEG_DATETIME));
        SimpleDateFormat dfs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date d1 = dfs.parse(ti.toJdbcString(DateTime.SEG_DATETIME_NAONS));
        System.out.println("d1=" + d1 + ", d1.getTime=" + d1.getTime() + ", ti.getNaons=" + ti.getNaons() + ", ");
        long t = 1519970951000L;
        String tim = String.valueOf(t);
        System.out.println(tim.substring(0, 10));
        System.out.println(tim.substring(10, tim.length()));
//
//        ZoneId zoneId = ZoneId.of(ti.getTimeZone());
//        TimeZone timeZone = TimeZone.getTimeZone(zoneId);
//        Calendar calendar = Calendar.getInstance();
//        calendar.setTimeZone(timeZone);
//        calendar.set(Calendar.YEAR, ti.getYear());
//        calendar.set(Calendar.MONTH, ti.getMonth());
//        calendar.set(Calendar.DAY_OF_MONTH, ti.getDay());
//        calendar.set(Calendar.HOUR, ti.getHour());
//        calendar.set(Calendar.MINUTE, ti.getMinute());
//        calendar.set(Calendar.SECOND, ti.getSecond());
//        calendar.set(Calendar.MILLISECOND, ti.getNaons());
//        System.out.println(calendar.getTime());
//
//        int offset = calendar.get(Calendar.ZONE_OFFSET);
//        int dstoff = calendar.get(Calendar.DST_OFFSET);
//        System.out.println("offset=" + offset + ", dstoff=" + dstoff);
//        calendar.add(Calendar.MILLISECOND, -(offset + dstoff));
//        System.out.println(calendar.get(Calendar.YEAR) + "-" + calendar.get(Calendar.MONTH)
//            + "-" + calendar.get(Calendar.DAY_OF_MONTH) + " " + calendar.get(Calendar.HOUR_OF_DAY)
//            + ":" + calendar.get(Calendar.MINUTE) + ":" + calendar.get(Calendar.SECOND) + "." + calendar.get(Calendar.MILLISECOND));
//        System.out.println(calendar.getTime());
//        Date date = new Date(calendar.getTimeInMillis());
//        System.out.println(timeZone);
//        System.out.println(timeZone.getRawOffset());
//        System.out.println(calendar.getTimeInMillis());
//        System.out.println(date.toGMTString());
    }

    @Test
    public void testEffectiveTimeZoneTest() throws ParseException {
        DateTime dateTime = new DateTime();
        assertTrue(dateTime.isEffectiveTimeZone("-08:00"));
        assertTrue(dateTime.isEffectiveTimeZone("+08:00"));
        assertTrue(dateTime.isEffectiveTimeZone("+08"));
        assertTrue(dateTime.isEffectiveTimeZone("+08:01:01"));
        assertTrue(dateTime.isEffectiveTimeZone("+8"));
        assertTrue(dateTime.isEffectiveTimeZone("Asia/shanghai"));
        assertFalse(dateTime.isEffectiveTimeZone("AM"));
    }

    @Test
    public void testParseJdbcDatetimeTest() throws ParseException {
        DateTime dateTime = new DateTime("0002-12-18 01:05:52+08:05:52 BC", DateTime.SEG_DATETIME_NAONS_TZ_ERA);
        Assert.assertEquals("0002-12-18 01:05:52.0 +08:05:52 BC", dateTime.toString());

        DateTime dateTime2 = new DateTime("0002-12-18 01:05:52+08:05:52", DateTime.SEG_DATETIME_NAONS_TZ_ERA);
        Assert.assertEquals("0002-12-18 01:05:52.0 +08:05:52", dateTime2.toString());
    }
}
