package com.aliyun.dts.subscribe.clients.record.fast;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BufferWarpBinaryDecoder extends Decoder {

    private byte[] buf = null;
    private int pos = 0;
    private int limit = 0;

    byte[] getBuf() {
        return buf;
    }

    public int getPos() {
        return pos;
    }

    public BufferWarpBinaryDecoder(byte[] data) {
        if (null == data) {
            throw new IllegalArgumentException("Input data array is null");
        }
        this.buf = data;
        this.pos = 0;
        this.limit = data.length;
    }

    @Override
    public void readNull() {
    }

    @Override
    public boolean readBoolean() throws IOException {
        ensureBounds(1);
        int n = buf[pos++] & 0xff;
        return n == 1;
    }

    @Override
    public int readInt() throws IOException {
        try {
            int len = 1;
            int b = buf[pos] & 0xff;
            int n = b & 0x7f;
            if (b > 0x7f) {
                b = buf[pos + len++] & 0xff;
                n ^= (b & 0x7f) << 7;
                if (b > 0x7f) {
                    b = buf[pos + len++] & 0xff;
                    n ^= (b & 0x7f) << 14;
                    if (b > 0x7f) {
                        b = buf[pos + len++] & 0xff;
                        n ^= (b & 0x7f) << 21;
                        if (b > 0x7f) {
                            b = buf[pos + len++] & 0xff;
                            n ^= (b & 0x7f) << 28;
                            if (b > 0x7f) {
                                throw new IOException("Invalid int encoding");
                            }
                        }
                    }
                }
            }
            pos += len;
            if (pos > limit) {
                throw new EOFException();
            }
            return (n >>> 1) ^ -(n & 1); // back to two's-complement
        } catch (ArrayIndexOutOfBoundsException ex) {
            throw new EOFException();
        }
    }

    @Override
    public long readLong() throws IOException {
        try {
            int b = buf[pos++] & 0xff;
            int n = b & 0x7f;
            long l;
            if (b > 0x7f) {
                b = buf[pos++] & 0xff;
                n ^= (b & 0x7f) << 7;
                if (b > 0x7f) {
                    b = buf[pos++] & 0xff;
                    n ^= (b & 0x7f) << 14;
                    if (b > 0x7f) {
                        b = buf[pos++] & 0xff;
                        n ^= (b & 0x7f) << 21;
                        if (b > 0x7f) {
                            // only the low 28 bits can be set, so this won't carry
                            // the sign bit to the long
                            l = innerLongDecode((long) n);
                        } else {
                            l = n;
                        }
                    } else {
                        l = n;
                    }
                } else {
                    l = n;
                }
            } else {
                l = n;
            }
            if (pos > limit) {
                throw new EOFException();
            }
            return (l >>> 1) ^ -(l & 1); // back to two's-complement
        } catch (ArrayIndexOutOfBoundsException ex) {
            throw new EOFException();
        }
    }

    // splitting readLong up makes it faster because of the JVM does more
    // optimizations on small methods
    private long innerLongDecode(long l) throws IOException {
        int len = 1;
        int b = buf[pos] & 0xff;
        l ^= (b & 0x7fL) << 28;
        if (b > 0x7f) {
            b = buf[pos + len++] & 0xff;
            l ^= (b & 0x7fL) << 35;
            if (b > 0x7f) {
                b = buf[pos + len++] & 0xff;
                l ^= (b & 0x7fL) << 42;
                if (b > 0x7f) {
                    b = buf[pos + len++] & 0xff;
                    l ^= (b & 0x7fL) << 49;
                    if (b > 0x7f) {
                        b = buf[pos + len++] & 0xff;
                        l ^= (b & 0x7fL) << 56;
                        if (b > 0x7f) {
                            b = buf[pos + len++] & 0xff;
                            l ^= (b & 0x7fL) << 63;
                            if (b > 0x7f) {
                                throw new IOException("Invalid long encoding");
                            }
                        }
                    }
                }
            }
        }
        pos += len;
        return l;
    }

    @Override
    public float readFloat() throws IOException {
        ensureBounds(4);
        int len = 1;
        int n = (buf[pos] & 0xff) | ((buf[pos + len++] & 0xff) << 8)
                | ((buf[pos + len++] & 0xff) << 16) | ((buf[pos + len++] & 0xff) << 24);
        if ((pos + 4) > limit) {
            throw new EOFException();
        }
        pos += 4;
        return Float.intBitsToFloat(n);
    }

    @Override
    public double readDouble() throws IOException {
        ensureBounds(8);
        int len = 1;
        int n1 = (buf[pos] & 0xff) | ((buf[pos + len++] & 0xff) << 8)
                | ((buf[pos + len++] & 0xff) << 16) | ((buf[pos + len++] & 0xff) << 24);
        int n2 = (buf[pos + len++] & 0xff) | ((buf[pos + len++] & 0xff) << 8)
                | ((buf[pos + len++] & 0xff) << 16) | ((buf[pos + len++] & 0xff) << 24);
        if ((pos + 8) > limit) {
            throw new EOFException();
        }
        pos += 8;
        return Double.longBitsToDouble((((long) n1) & 0xffffffffL)
                | (((long) n2) << 32));
    }

    @Override
    public Utf8 readString(Utf8 old) throws IOException {
        int length = readInt();
        Utf8 result = (old != null ? old : new Utf8());
        result.setByteLength(length);
        if (0 != length) {
            doReadBytes(result.getBytes(), 0, length);
        }
        return result;
    }

    private final Utf8 scratchUtf8 = new Utf8();

    @Override
    public String readString() throws IOException {
        return readString(scratchUtf8).toString();
    }

    public String readAsciiString() throws IOException {
        int length = readInt();
        ensureBounds(length);
        char[] newChars = new char[length];
        for (int i = 0; i < newChars.length; ++i) {
            newChars[i] = (char) this.buf[i + this.pos];
        }
        this.pos += length;
        return new String(newChars);
    }

    @Override
    public void skipString() throws IOException {
        doSkipBytes(readInt());
    }

    @Override
    public ByteBuffer readBytes(ByteBuffer old) throws IOException {
        int length = readInt();
        ByteBuffer result;
        if (old != null && length <= old.capacity()) {
            result = old;
            result.clear();
        } else {
            result = ByteBuffer.allocate(length);
        }
        doReadBytes(result.array(), result.position(), length);
        result.limit(length);
        return result;
    }

    @Override
    public void skipBytes() throws IOException {
        doSkipBytes(readInt());
    }

    @Override
    public void readFixed(byte[] bytes, int start, int length) throws IOException {
        doReadBytes(bytes, start, length);
    }

    @Override
    public void skipFixed(int length) throws IOException {
        doSkipBytes(length);
    }

    @Override
    public int readEnum() throws IOException {
        return readInt();
    }

    protected void doSkipBytes(long length) throws IOException {
        int remaining = limit - pos;
        if (length <= remaining) {
            pos += length;
        } else {
            throw new EOFException();
        }
    }

    /**
     * Reads <tt>length</tt> bytes into <tt>bytes</tt> starting at <tt>start</tt>.
     *
     * @throws EOFException If there are not enough number of bytes in the source.
     * @throws IOException
     */
    protected void doReadBytes(byte[] bytes, int start, int length)
            throws IOException {
        if (length < 0) {
            throw new AvroRuntimeException("Malformed data. Length is negative: " + length);
        }
        ensureBounds(length);
        System.arraycopy(buf, pos, bytes, start, length);
        pos += length;
    }

    /**
     * Returns the number of items to follow in the current array or map. Returns
     * 0 if there are no more items in the current array and the array/map has
     * ended.
     *
     * @throws IOException
     */
    protected long doReadItemCount() throws IOException {
        long result = readLong();
        if (result < 0) {
            readLong(); // Consume byte-count if present
            result = -result;
        }
        return result;
    }

    /**
     * Reads the count of items in the current array or map and skip those items,
     * if possible. If it could skip the items, keep repeating until there are no
     * more items left in the array or map. If items cannot be skipped (because
     * byte count to skip is not found in the stream) return the count of the
     * items found. The client needs to skip the items individually.
     *
     * @return Zero if there are no more items to skip and end of array/map is
     * reached. Positive number if some items are found that cannot be
     * skipped and the client needs to skip them individually.
     * @throws IOException
     */
    private long doSkipItems() throws IOException {
        long result = readInt();
        while (result < 0) {
            long bytecount = readLong();
            doSkipBytes(bytecount);
            result = readInt();
        }
        return result;
    }

    @Override
    public long readArrayStart() throws IOException {
        return doReadItemCount();
    }

    @Override
    public long arrayNext() throws IOException {
        return doReadItemCount();
    }

    @Override
    public long skipArray() throws IOException {
        return doSkipItems();
    }

    @Override
    public long readMapStart() throws IOException {
        return doReadItemCount();
    }

    @Override
    public long mapNext() throws IOException {
        return doReadItemCount();
    }

    @Override
    public long skipMap() throws IOException {
        return doSkipItems();
    }

    @Override
    public int readIndex() throws IOException {
        return readInt();
    }

    /**
     * Returns true if the current BufferWarpBinaryDecoder is at the end of its source data and
     * cannot read any further without throwing an EOFException or other
     * IOException.
     * <p/>
     * Not all implementations of BufferWarpBinaryDecoder support isEnd(). Implementations that do
     * not support isEnd() will throw a
     * {@link UnsupportedOperationException}.
     */
    public boolean isEnd() {
        return limit - pos <= 0;
    }

    /**
     * Ensures that buf[pos + num - 1] is not out of the buffer array bounds.
     * However, buf[pos + num -1] may be >= limit if there is not enough data left
     * in the source to fill the array with num bytes.
     * <p/>
     * This method allows readers to read ahead by num bytes safely without
     * checking for EOF at each byte. However, readers must ensure that their
     * reads are valid by checking that their read did not advance past the limit
     * before adjusting pos.
     * <p/>
     * num must be less than the buffer size and greater than 0
     */
    private void ensureBounds(int num) throws IOException {
        int remaining = limit - pos;
        if (remaining < num) {
            throw new EOFException();
        }
    }
}

