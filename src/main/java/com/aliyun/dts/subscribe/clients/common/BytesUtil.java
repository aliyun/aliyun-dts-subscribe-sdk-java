package com.aliyun.dts.subscribe.clients.common;

import com.google.common.primitives.UnsignedBytes;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class BytesUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(BytesUtil.class);

    /**
     * Size of boolean in bytes
     */
    public static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;

    /**
     * Size of byte in bytes
     */
    public static final int SIZEOF_BYTE = SIZEOF_BOOLEAN;

    /**
     * Size of char in bytes
     */
    public static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;

    /**
     * Size of double in bytes
     */
    public static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;

    /**
     * Size of float in bytes
     */
    public static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;

    /**
     * Size of int in bytes
     */
    public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

    /**
     * Size of long in bytes
     */
    public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

    /**
     * Size of short in bytes
     */
    public static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;

    private static final String UTF8_ENCODING = "UTF-8";

    private static final char[] HEX_DIGITS = new char[]{
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    private static boolean isValidEncoding(String encoding) {
        if (StringUtils.isEmpty(encoding)) {
            return false;
        }
        if (StringUtils.equalsIgnoreCase(encoding, "null")) {
            return false;
        }
        return true;
    }

    public static byte[] toBytes(long val) {
        byte[] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    public static byte[] toBytes(int val) {
        byte[] b = new byte[4];
        for (int i = 3; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    public static byte[] toBytes(final boolean b) {
        return new byte[]{b ? (byte) (-1) : (byte) 0};
    }

    public static byte[] toBytes(final float f) {
        // Encode it as int
        return toBytes(Float.floatToRawIntBits(f));
    }

    public static byte[] toBytes(final double d) {
        return toBytes(Double.doubleToRawLongBits(d));
    }

    public static byte[] toBytes(BigDecimal val) {
        byte[] valueBytes = val.unscaledValue().toByteArray();
        byte[] result = new byte[valueBytes.length + SIZEOF_INT];
        int offset = putInt(result, 0, val.scale());
        putBytes(result, offset, valueBytes, 0, valueBytes.length);
        return result;
    }

    public static byte[] toBytes(String s) {
        if (s == null) {
            return null;
        }

        try {
            return s.getBytes(UTF8_ENCODING);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("UTF-8 not supported?", e);
            return null;
        }
    }

    public static byte[] toBytes(String value, String encoding) throws UnsupportedEncodingException {
        if (value == null) {
            return null;
        }

        String realEncoding = UTF8_ENCODING;
        if (isValidEncoding(encoding)) {
            realEncoding = encoding;
        }

        try {
            return value.getBytes(realEncoding);
        } catch (UnsupportedEncodingException foo) {
            String mappedEncoding = JDKCharsetMapper.getJDKECharset(realEncoding);
            LOGGER.warn("get bytes from from using encoding {} failed, just try to use encoding {} again", realEncoding, mappedEncoding);

            return value.getBytes(mappedEncoding);
        }
    }

    public static byte[] toBytes(final BigInteger bi) {
        if (bi == null) {
            return null;
        }
        return bi.toByteArray();
    }

    public static long toLong(byte[] bytes) {
        return toLong(bytes, 0, SIZEOF_LONG);
    }

    public static long toLong(byte[] bytes, int offset, final int length) {
        long l = 0;
        for (int i = offset; i < offset + length; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }

    public static int toInt(byte[] bytes) {
        return toInt(bytes, 0, SIZEOF_INT);
    }

    public static int toInt(byte[] bytes, int offset) {
        return toInt(bytes, offset, SIZEOF_INT);
    }

    public static int toInt(byte[] bytes, int offset, final int length) {
        int n = 0;
        for (int i = offset; i < (offset + length); i++) {
            n <<= 8;
            n ^= bytes[i] & 0xFF;
        }
        return n;
    }

    public static boolean toBoolean(final byte[] b) {
        return b[0] != (byte) 0;
    }

    public static float toFloat(byte[] bytes) {
        return toFloat(bytes, 0);
    }

    public static float toFloat(byte[] bytes, int offset) {
        return Float.intBitsToFloat(toInt(bytes, offset, SIZEOF_INT));
    }

    public static double toDouble(final byte[] bytes) {
        return toDouble(bytes, 0);
    }

    public static double toDouble(final byte[] bytes, final int offset) {
        return Double.longBitsToDouble(toLong(bytes, offset, SIZEOF_LONG));
    }

    public static BigDecimal toBigDecimal(byte[] bytes) {
        return toBigDecimal(bytes, 0, bytes.length);
    }

    public static BigDecimal toBigDecimal(byte[] bytes, int offset, final int length) {
        if (bytes == null || length < SIZEOF_INT + 1
                || (offset + length > bytes.length)) {
            return null;
        }

        int scale = toInt(bytes, offset);
        byte[] tcBytes = new byte[length - SIZEOF_INT];
        System.arraycopy(bytes, offset + SIZEOF_INT, tcBytes, 0, length - SIZEOF_INT);
        return new BigDecimal(new BigInteger(tcBytes), scale);
    }

    public static int putInt(byte[] bytes, int offset, int val) {
        if (bytes.length - offset < SIZEOF_INT) {
            throw new IllegalArgumentException("Not enough room to put an int at"
                    + " offset " + offset + " in a " + bytes.length + " byte array");
        }
        for (int i = offset + 3; i > offset; i--) {
            bytes[i] = (byte) val;
            val >>>= 8;
        }
        bytes[offset] = (byte) val;
        return offset + SIZEOF_INT;
    }

    public static int putBytes(byte[] tgtBytes, int tgtOffset, byte[] srcBytes,
                               int srcOffset, int srcLength) {
        System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
        return tgtOffset + srcLength;
    }

    public static String toString(byte[] bytes, String encoding) throws UnsupportedEncodingException {
        if (bytes == null) {
            return null;
        }

        if (bytes.length == 0) {
            return "";
        }

        String realEncoding = UTF8_ENCODING;
        if (isValidEncoding(encoding)) {
            realEncoding = encoding;
        }

        try {
            return new String(bytes, realEncoding);
        } catch (UnsupportedEncodingException foo) {
            String mappedEncoding = JDKCharsetMapper.getJDKECharset(realEncoding);
            LOGGER.warn("get bytes from from using encoding {} failed, just try to use encoding {} again", realEncoding, mappedEncoding);

            return new String(bytes, mappedEncoding);
        }
    }

    public static boolean equals(final byte[] left, final byte[] right) {
        // Could use Arrays.equals?
        //noinspection SimplifiableConditionalExpression
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        if (left.length != right.length) {
            return false;
        }
        if (left.length == 0) {
            return true;
        }

        // Since we're often comparing adjacent sorted data,
        // it's usual to have equal arrays except for the very last byte
        // so check that first
        if (left[left.length - 1] != right[right.length - 1]) {
            return false;
        }

        return compareTo(left, right) == 0;
    }

    public static int compareTo(final byte[] left, final byte[] right) {
        if (left == null) {
            return right == null ? 0 : -1;
        } else if (right == null) {
            return 1;
        }

        return UnsignedBytes.lexicographicalComparator().compare(left, right);
    }

    public static BigInteger toBigInteger(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        return new BigInteger(bytes);
    }

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

    public static String bytesToHexString(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
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

    public static byte[] hexStringToBytes(String hexString) {
        return hexStringToByteBuffer(hexString).array();
    }

    public static ByteBuffer newInitialByteBuffer(ByteBuffer binaryData) {
        byte[] newBytes = new byte[binaryData.capacity()];

        if (binaryData.hasArray()) {
            System.arraycopy(binaryData.array(), binaryData.arrayOffset(), newBytes, 0, binaryData.capacity());
            return ByteBuffer.wrap(newBytes, binaryData.position(), binaryData.remaining());
        } else {
            int pos = binaryData.position();
            int len = binaryData.limit() - pos;
            byte[] b = new byte[len];
            binaryData.duplicate().get(b, 0, len);
            return ByteBuffer.wrap(b);
        }
    }
}
