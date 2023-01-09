package com.taobao.drc.client.message.dts.record.utils;

import java.nio.ByteBuffer;

public class BytesUtil {
    public static final String UTF8_ENCODING = "UTF-8";
    private static final char[] HEX_DIGITS = new char[]{
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    public static String byteBufferToHexString(ByteBuffer byteBuffer) {
        StringBuilder stringBuilder = new StringBuilder(2 * (byteBuffer.limit() - byteBuffer.position()));
        for (int i = byteBuffer.position(); i < byteBuffer.limit(); ++i) {
            byte b = byteBuffer.get(i);
            int lowBits = (b & 0x0F);
            int highBits = (b & 0xF0) >> 4;
            stringBuilder.append(HEX_DIGITS[highBits]);
            stringBuilder.append(HEX_DIGITS[lowBits]);
        }
        return stringBuilder.toString();
    }

    public static ByteBuffer hexStringToByteBuffer(String hexString) {
        char[] chars = hexString.toCharArray();
        byte byteData = 0;

        ByteBuffer byteBuffer = ByteBuffer.allocate((chars.length + 1) >> 1);

        for (int i = 0; i < chars.length; i++) {
            if (chars[i] < 'A') {
                // chars[i] is a number
                byteData |= (chars[i] - '0') & 0x0F;
            } else if (chars[i] < 'a') {
                // chars[i] is a upper alphabet
                byteData |= (10 + (chars[i] - 'A')) & 0x0F;
            } else {
                // chars[i] is a lower alphabet
                byteData |= (10 + (chars[i] - 'a')) & 0x0F;
            }

            if (0 != (i & 0x01)) {
                // compose a whole byte
                byteBuffer.put(byteData);
                byteData = 0;
            } else {
                byteData = (byte) (byteData << (byte) 4);
            }
        }

        if (0 != (chars.length & 0x01)) {
            byteBuffer.put(byteData);
        }

        return byteBuffer;
    }
}
