package com.taobao.drc.togo.util;

/**
 * Created by longxuan on 18/1/17.
 */
public class BitUtil {
    public static long doubleIntToLong(int highValue, int lowValue) {
        return ((highValue & 0xFFFFFFFFL) << 32) | (lowValue & 0xFFFFFFFFL);
    }

    public static long formatHashRegionIDAndRecordType(int hashValue, short regionID, short recordType) {
        return ((hashValue & 0xFFFFFFFFL) << 32) | ((regionID & 0x0000FFFFL) << 16) | (recordType & 0x0000FFFF);
    }

    public static short readRegionID(long v) {
        return (short)(v >> 16);
    }

    public static int readHashID(long v) {
        return (int)(v >> 32);
    }

    public static short readRecordType(long v) {
        return (short)v;
    }

    public static int readHighValueFromInt(long value) {
        return (int)((value) >> 32);
    }

    public static int readLowValueFromInt(long value) {
        return (int)((value));
    }
}
