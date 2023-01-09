package com.taobao.drc.client.message.dts.record.value;

import org.apache.commons.lang3.StringUtils;
import sun.util.calendar.ZoneInfoFile;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DateTime implements Value<String> {

    private static Set<String> timeZoneNames = new HashSet<String>();
    private static Set<String> commonEraNames = new HashSet<String>(17);

    static {
        for (Map.Entry<String, String> kv : ZoneInfoFile.getAliasMap().entrySet()) {
            timeZoneNames.add(kv.getKey().toLowerCase());
            timeZoneNames.add(kv.getValue().toLowerCase());
        }
        commonEraNames.add("AD");
        commonEraNames.add("ad");
        commonEraNames.add("bc");
        commonEraNames.add("BC");

        commonEraNames.add(" AD");
        commonEraNames.add(" ad");
        commonEraNames.add(" bc");
        commonEraNames.add(" BC");
    }

    private static final String BC = "BC";
    private static final String AD = "AD";

    public static final int SEG_NEGATIVE = 0x1;
    public static final int SEG_YEAR = 0x2;
    public static final int SEG_MONTH = 0x4;
    public static final int SEG_DAY = 0x8;

    public static final int SEG_HOUR = 0x10;
    public static final int SEG_MINITE = 0x20;
    public static final int SEG_SECOND = 0x40;

    public static final int SEG_NAONS = 0x80;
    public static final int SEG_TIMEZONE = 0x100;
    public static final int SEG_COMMON_ERA = 0x200;

    public static final int SEG_TIME = SEG_HOUR | SEG_MINITE | SEG_SECOND;
    public static final int SEG_TIME_NAONS = SEG_TIME | SEG_NAONS;
    public static final int SEG_DATE = SEG_YEAR | SEG_MONTH | SEG_DAY;
    public static final int SEG_DATETIME = SEG_DATE | SEG_TIME;
    public static final int SEG_DATETIME_NAONS = SEG_DATETIME | SEG_NAONS;
    public static final int SEG_DATETIME_NAONS_TZ = SEG_DATETIME_NAONS | SEG_TIMEZONE;
    public static final int SEG_DATETIME_NAONS_TZ_ERA = SEG_DATETIME_NAONS_TZ | SEG_COMMON_ERA;

    private int year;
    private int month;
    private int day;
    private int hour;
    private int minute;
    private int second;
    private int naons;

    private String datetime;

    private String timeZone;
    private String commonEra;
    private int segments; // bitmap(negative,year,day,hour,minite,second,micro,naons,timeZone)

    public DateTime() {
    }

    public DateTime(String jdbcDatetime, int segments) {
        this.segments = segments;
        this.parseJdbcDatetime(jdbcDatetime);
    }

    @Override
    public ValueType getType() {
        return ValueType.DATETIME;
    }

    @Override
    public String getData() {
        return toString();
    }

    public boolean isNegative() {
        return 0 != (this.segments & SEG_NEGATIVE);
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        if (year < 0) {
            this.setSegments(SEG_NEGATIVE);
        }
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        if (day < 0) {
            this.setSegments(SEG_NEGATIVE);
        }
        this.day = day;
    }

    public int getHour() {
        return hour;
    }

    public void setHour(int hour) {
        if (hour < 0) {
            this.setSegments(SEG_NEGATIVE);
        }
        this.hour = hour;
    }

    public int getMinute() {
        return minute;
    }

    public void setMinute(int minute) {
        if (minute < 0) {
            this.setSegments(SEG_NEGATIVE);
        }
        this.minute = minute;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        if (second < 0) {
            this.setSegments(SEG_NEGATIVE);
        }
        this.second = second;
    }

    public int getNaons() {
        return naons;
    }

    public void setNaons(int naons) {
        if (naons < 0) {
            this.setSegments(SEG_NEGATIVE);
        }
        this.naons = naons;
    }

    public String getCommonEra() {
        return this.commonEra;
    }

    public void setCommonEra(String commonEra) {
        if (null != commonEra) {
            this.setSegments(SEG_COMMON_ERA);
            this.commonEra = commonEra;
        }
    }

    public String getDatetime() {
        return datetime;
    }

    public void setDatetime(String datetime) {
        this.datetime = datetime;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public int getSegments() {
        return segments;
    }

    public void setSegments(int segments) {
        this.segments |= segments;
    }

    public void clearSegments(int segments) {
        this.segments &= (~segments);
    }

    @Override
    public String toString() {
        return toJdbcString(this.segments);
    }

    public boolean isSet(int segPart) {
        return isSet(this.segments, segPart);
    }

    public boolean isSet(int segments, int segPart) {
        return 0 != (segments & segPart);
    }

    public UnixTimestamp toUnixTimestampValue() throws ParseException {
        long timestamp = toUnixTimestamp();
        String timestampString = String.valueOf(timestamp);
        String timestampSec = timestampString.substring(0, 10);
        String timestampMicro = "0";
        if (timestampString.length() > 10) {
            timestampMicro = timestampString.substring(10, timestampString.length());
        }
        return new UnixTimestamp(Long.parseLong(timestampSec), Integer.parseInt(timestampMicro));
    }

    public long toUnixTimestamp() throws ParseException {
        String parseTimeZone = getTimeZone();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        if (null == parseTimeZone || parseTimeZone.trim().isEmpty()) {
            Date date = sdf.parse(toJdbcString(DateTime.SEG_DATETIME_NAONS));
            return date.getTime();
        }

        // "GMT+0:00" => "GMT+00:00"
        if (parseTimeZone.startsWith("GMT") && parseTimeZone.substring(3).length() == 5) {
            parseTimeZone = "GMT+0" + parseTimeZone.substring(4);
        }
        // "+0:00"=>"+00:00"
        if (parseTimeZone.length() == 5) {
            parseTimeZone = "+0" + parseTimeZone.substring(1);
        }
        TimeZone timeZone = TimeZone.getTimeZone(parseTimeZone);
        sdf.setTimeZone(timeZone);
        Date date = sdf.parse(toJdbcString(DateTime.SEG_DATETIME_NAONS));
        return date.getTime() - timeZone.getRawOffset();
    }

    public String toJdbcString(int segments) {

        StringBuffer datatimeBuf = new StringBuffer(32);
        if (isSet(segments, SEG_YEAR)) {
            int yearV = this.year > 0 ? this.year : -this.year;
            String yearZeros = "0000";
            String yearString = Integer.toString(yearV);
            if (yearV < 1000) {
                yearString = yearZeros.substring(0, (4 - yearString.length())) + yearString;
            }
            datatimeBuf.append(yearString);
        }

        if (isSet(segments, SEG_MONTH)) {
            datatimeBuf.append("-");
            int monthV = this.month > 0 ? this.month : -this.month;
            if (monthV < 10) {
                datatimeBuf.append("0").append(monthV);
            } else {
                datatimeBuf.append(monthV);
            }
        }

        if (isSet(segments, SEG_DAY)) {
            if (datatimeBuf.length() > 0) {
                datatimeBuf.append("-");
            }
            int dayV = this.day > 0 ? this.day : -this.day;
            if (dayV < 10) {
                datatimeBuf.append("0").append(dayV);
            } else {
                datatimeBuf.append(dayV);
            }
        }

        if (isSet(segments, SEG_HOUR)) {
            if (datatimeBuf.length() > 0) {
                datatimeBuf.append(" ");
            }
            int hourV = this.hour > 0 ? this.hour : -this.hour;
            if (hourV < 10) {
                datatimeBuf.append("0").append(hourV);
            } else {
                datatimeBuf.append(hourV);
            }
        }

        if (isSet(segments, SEG_MINITE)) {
            datatimeBuf.append(":");
            int minuteV = this.minute > 0 ? this.minute : -this.minute;
            if (minuteV < 10) {
                datatimeBuf.append("0").append(minuteV);
            } else {
                datatimeBuf.append(minuteV);
            }
        }

        if (isSet(segments, SEG_SECOND)) {
            datatimeBuf.append(":");
            int secondV = this.second > 0 ? this.second : -this.second;
            if (secondV < 10) {
                datatimeBuf.append("0").append(secondV);
            } else {
                datatimeBuf.append(secondV);
            }
        }

        if (isSet(segments, SEG_NAONS)) {
            String zeros = "000000000";
            int naonsV = this.naons > 0 ? this.naons : -this.naons;
            String naonsString = Integer.toString(naonsV);
            naonsString = zeros.substring(0, (9 - naonsString.length())) + naonsString;

            char[] nanosChar = new char[naonsString.length()];
            naonsString.getChars(0, naonsString.length(), nanosChar, 0);
            int truncIndex = 8;
            while (nanosChar[truncIndex] == '0' && truncIndex > 0) {
                truncIndex--;
            }
            naonsString = new String(nanosChar, 0, truncIndex + 1);

            //when nanosString is 0, do not output it
            if (!"0".equals(naonsString)) {
                datatimeBuf.append(".").append(naonsString);
            }
        }

        if (isSet(segments, SEG_TIMEZONE) && timeZone != null) {
            datatimeBuf.append(" ").append(timeZone);
        }

        if (isSet(segments, SEG_COMMON_ERA) && null != commonEra) {
            datatimeBuf.append(" ").append(commonEra);
        }

        if (isSet(segments, SEG_NEGATIVE)) {
            return "-" + datatimeBuf.toString();
        }
        return datatimeBuf.toString();
    }

    protected final int upMicroToNaons(int time, int n) {
        for (int i = 0; i < n; ++i) {
            time *= 10;
        }
        return time;
    }

    protected boolean isEffectiveTimeZone(String timeZone) {
        if (null == timeZone || timeZone.length() <= 0) {
            return false;
        }
        if (StringUtils.startsWith(timeZone, "GMT")
                || StringUtils.startsWith(timeZone, "UTC")) {
            return true;
        }
        char c = timeZone.charAt(0);
        if ('-' == c || '+' == c) {
            for (int i = 1; i < timeZone.length(); ++i) {
                if (('0' > timeZone.charAt(i) || '9' < timeZone.charAt(i)) && ':' != timeZone.charAt(i)) {
                    return false;
                }
            }
            return true;
        } else {
            return timeZoneNames.contains(timeZone.toLowerCase());
        }
    }

    protected void parseJdbcDatetime(String datetime) {
        if (null == datetime || datetime.trim().isEmpty()) {
            throw new IllegalArgumentException("datetime is null or empty.");
        }
        if (isSet(SEG_COMMON_ERA) && datetime.length() > 2) {
            String commonEra = datetime.substring(datetime.length() - 2);
            if (commonEraNames.contains(commonEra)) {
                this.setCommonEra(commonEra);
                datetime = datetime.substring(0, datetime.length() - 2).trim();
            }
        }
        if (isSet(SEG_TIMEZONE)) {
            int index = -1;
            for (int i = datetime.length() - 1; i >= 0; --i) {
                char c = datetime.charAt(i);
                if (' ' == c || '+' == c || '-' == c) {
                    if (i > 1) {
                        char lc = datetime.charAt(i - 1);
                        if ((lc >= 'a' && lc <= 'z') || (lc >= 'A' && lc <= 'Z')) {
                            continue;
                        }
                    }
                    index = i;
                    break;
                }
            }
            if (index >= 0) {
                String timeZone = datetime.substring(index);
                if (isEffectiveTimeZone(timeZone.trim())) {
                    this.setTimeZone(timeZone.trim());
                    datetime = datetime.substring(0, datetime.length() - timeZone.length()).trim();
                }
            }
        }
        int[] ret = new int[7];
        int j = 0, m = 0, n = 0;
        byte[] bytes = datetime.getBytes();
        boolean microMode = false;
        for (int i = 0; i < bytes.length && j < bytes.length; ++i) {
            if ('0' <= bytes[i] && '9' >= bytes[i]) {
                m *= 10;
                m += bytes[i] - '0';
                n += 1;
            } else if (0 != n) {
                ret[j] = m;
                if (microMode) {
                    ret[j] = upMicroToNaons(m, 9 - n);
                }
                m = 0;
                n = 0;
                ++j;

                if ('.' == bytes[i]) {
                    microMode = true;
                } else {
                    microMode = false;
                }
            }
        }
        if (n != 0) {
            ret[j] = m;
            if (microMode) {
                ret[j] = upMicroToNaons(m, 9 - n);
            }
        }

        int index = 0;
        if (isSet(SEG_YEAR)) {
            this.setYear(ret[index++]);
        }
        if (isSet(SEG_MONTH)) {
            this.setMonth(ret[index++]);
        }
        if (isSet(SEG_DAY)) {
            this.setDay(ret[index++]);
        }

        if (isSet(SEG_HOUR)) {
            this.setHour(ret[index++]);
        }
        if (isSet(SEG_MINITE)) {
            this.setMinute(ret[index++]);
        }
        if (isSet(SEG_SECOND)) {
            this.setSecond(ret[index++]);
        }

        if (isSet(SEG_NAONS)) {
            this.setNaons(ret[index++]);
        }

        if ('-' == bytes[0]) {
            setSegments(SEG_NEGATIVE);
        }
    }

    public static boolean isZeroDate(String value) {
        return "0000-00-00".equalsIgnoreCase(value)
            || "0000-00-00 00:00:00".equalsIgnoreCase(value)
            || "0000-00-00 00:00:00.0".equalsIgnoreCase(value)
            || "0000-00-00 00:00:00.00".equalsIgnoreCase(value)
            || "0000-00-00 00:00:00.000".equalsIgnoreCase(value)
            || "0000-00-00 00:00:00.0000".equalsIgnoreCase(value)
            || "0000-00-00 00:00:00.00000".equalsIgnoreCase(value)
            || "0000-00-00 00:00:00.000000".equalsIgnoreCase(value);
    }

    @Override
    public long size() {
        return StringUtils.length(datetime) + StringUtils.length(timeZone) + Integer.BYTES * 8;
    }

    @Override
    public DateTime parse(Object rawData) {
        DateTime newDateTime = new DateTime();
        newDateTime.parseJdbcDatetime(rawData.toString());
        return newDateTime;
    }
}
