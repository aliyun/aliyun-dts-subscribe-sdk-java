package com.taobao.drc.togo.util;

/**
 * Created by longxuan on 18/1/17.
 */
public class HashUtil {
    /**
     * copy from https://lark.alipay.com/shaojin.wensj/xv69in/kykk2w
     */
    public static long fnv1a64IncrementWithSeparator(long basic, char separator, String chars) {
        long hashCode = basic;
        // hash with separator
        hashCode ^= separator;
        hashCode *= 0x100000001b3L;
        int length = chars.length();
        // hash with following chars
        for (int i = 0; i < length; ++i) {
            char ch = chars.charAt(i);
            hashCode ^= ch;
            hashCode *= 0x100000001b3L;
        }

        return hashCode;
    }

    public static long fnv1a64IncrementWithSeparator(long basic, char separator, ByteString chars) {
        long hashCode = basic;
        // hash with separator
        hashCode ^= separator;
        hashCode *= 0x100000001b3L;
        byte[] bytes = chars.getBytes();
        int beginOffset = chars.getOffset();
        int endPos  = beginOffset + chars.getLength();
        // hash with following chars
        for (int i = beginOffset; i < endPos; ++i) {
            char ch = (char) (bytes[i] & 0xFF);

            hashCode ^= ch;
            hashCode *= 0x100000001b3L;
        }
        return hashCode;
    }



    // make sure objects length is greater than 2
    public static long fnv1a64IncrementArray(char slicer, Object... objects) {
        long base = fnv1a64(objects[0].toString());
        int indexGuard = objects.length - 1;
        for (int i = 1; i < objects.length; ++i) {
            base = fnv1a64IncrementWithSeparator(base, slicer, objects[i].toString());
        }
        return base;
    }

    public static long fnv1a64IncrementByteStringArray(char slicer, Object... objects) {
        long base = fnv1a64((ByteString) objects[0]);
        int indexGuard = objects.length - 1;
        for (int i = 1; i < objects.length; ++i) {
            base = fnv1a64IncrementWithSeparator(base, slicer, (ByteString)objects[i]);
        }
        return base;
    }

    public static long fnv1a64(ByteString chars) {
        long hash = 0xcbf29ce484222325L;
        byte[] bytes = chars.getBytes();
        int beginOffset = chars.getOffset();
        int endPos = beginOffset + chars.getLength();
        // hash with following chars
        for (int i = beginOffset; i < endPos; i++) {
            char ch = (char) (bytes[i] & 0xFF);
            hash ^= ch;
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    public static long fnv1a64(String chars) {
        long hash = 0xcbf29ce484222325L;
        int length = chars.length();
        for (int i = 0; i < length; ++i) {
            char c = chars.charAt(i);
            hash ^= c;
            hash *= 0x100000001b3L;
        }
        return hash;
    }

}
