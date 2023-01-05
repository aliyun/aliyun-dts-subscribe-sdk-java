package com.taobao.drc.client.utils;

public class MessageV1 {

    public static boolean isBigMsg(byte x) {
        return (((short)x & 0xFF) >> 7) == 1;
    }

    public static int getHeaderLen(byte[] dstBuf) {
        int value = 0;
        value |= (dstBuf[0] & 0x0000000F) << 24;
        value |= (dstBuf[1] & 0x000000FF) << 16;
        value |= (dstBuf[2] & 0x000000FF) << 8;
        value |= (dstBuf[3] & 0x000000FF);
        return value;
    }

    public static long getInt32(byte[] dstBuf, int offset) {
        long value = 0;
        value |= (dstBuf[offset] & 0x00000000FF) << 24;
        value |= (dstBuf[offset + 1] & 0x000000FF) << 16;
        value |= (dstBuf[offset + 2] & 0x000000FF) << 8;
        value |= (dstBuf[offset + 3] & 0x000000FF);
        return value;
    }

    public static boolean isBigMsgEnd(byte x) {
        return (((short)x &0xFF) >> 4) == 0x0b;
    }
}
