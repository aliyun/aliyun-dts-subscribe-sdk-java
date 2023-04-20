package com.taobao.drc.togo.util;

public class HexUtil {
    public static byte[] ANSI_ARRAY = new byte[256];
    static {
        for (int i = '0'; i <= '9'; ++i) {
            ANSI_ARRAY[i] = (byte)(i - '0');
        }
        for (int i = 'a'; i <= 'f'; ++i) {
            ANSI_ARRAY[i] = (byte)(10 + i - 'a');
        }
        for (int i = 'A'; i <= 'F'; ++i) {
            ANSI_ARRAY[i] = (byte)(10 + i - 'A');
        }
    }

    public static byte[] hexStringToBytes(String hexString, int beginOffset) {
        int byteLength = (hexString.length() - beginOffset) / 2;
        byte[] retBytes = new byte[byteLength];
        byte[] stringArray = hexString.getBytes();
        for (int i = 0; i < retBytes.length; ++i) {
            int offset = beginOffset + i * 2;
            retBytes[i] = (byte)((ANSI_ARRAY[stringArray[offset]] << 4) + (ANSI_ARRAY[stringArray[offset + 1]]));
        }
        return retBytes;
    }
}
