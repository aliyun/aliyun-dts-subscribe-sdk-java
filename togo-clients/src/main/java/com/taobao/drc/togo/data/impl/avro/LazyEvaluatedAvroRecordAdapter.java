package com.taobao.drc.togo.data.impl.avro;

import com.taobao.drc.togo.data.AbstractSchemafulRecord;
import com.taobao.drc.togo.data.DataException;
import com.taobao.drc.togo.data.schema.Field;
import com.taobao.drc.togo.data.schema.Schema;
import com.taobao.drc.togo.data.schema.SchemaConverter;
import com.taobao.drc.togo.util.ByteString;
import com.taobao.drc.togo.util.Charsets;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.taobao.drc.togo.data.impl.avro.AvroDataSerDeConstants.BYTE_ORDER;

/**
 * @author yangyang
 * @since 17/3/20
 */
public class LazyEvaluatedAvroRecordAdapter extends AbstractSchemafulRecord {
    private final SchemaConverter<org.apache.avro.Schema> converter;
    private final Schema schema;
    private final ByteBuffer byteBuffer;
    private final Object[] values;
    private final int fieldsStartOffset;
    private final byte[] buf;
    private int pos;

    LazyEvaluatedAvroRecordAdapter(SchemaConverter<org.apache.avro.Schema> converter, Schema schema, ByteBuffer byteBuffer) {
        this.converter = converter;
        this.schema = schema;
        this.byteBuffer = byteBuffer.order(BYTE_ORDER);
        int fieldNum = schema.fields().size();
        this.values = new Object[fieldNum];
        this.buf = byteBuffer.array();

        fieldsStartOffset = AvroDataSerDeConstants.fieldIndexSize(fieldNum);
    }

    @Override
    public Object get(String key) {
        return get(getFieldIndex(key));
    }

    @Override
    public LazyEvaluatedAvroRecordAdapter put(String key, Object t) {
        throw new UnsupportedOperationException("Lazy evaluated record is not writable!");
    }

    @Override
    public Object get(int index) {
        Object ret = values[index];
        if (ret == null) {
            Field field = schema.fields().get(index);
            try {
                int fieldOffset = byteBuffer.getInt(byteBuffer.position() + AvroDataSerDeConstants.fieldOffset(index));
                pos = byteBuffer.position() + fieldsStartOffset + fieldOffset;
                int remaining = byteBuffer.limit() - pos;
                pos = byteBuffer.arrayOffset() + pos;
                ret = read(pos, remaining, field);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to get field [" + index + "] of schema " + schema.name(), e);
            }
            values[index] = ret;
        }
        return ret;
    }

    private Object read(int offset, int length, Field field) throws IOException {
        switch (field.schema().type()) {
            case BOOLEAN:
                return readBoolean();
            case INT32:
                return readInt();
            case INT64:
                return readLong();
            case FLOAT32:
                return readFloat();
            case FLOAT64:
                return readDouble();
            case STRING:
                return readString();
            case BYTES:
                return readBytes();
            default:
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(buf, offset, length, null);
                GenericDatumReader<Object> reader = new GenericDatumReader<>(converter.toImpl(field.schema()));
                return reader.read(null, decoder);
        }
    }

    private boolean readBoolean() {
        int n = buf[pos++] & 0xff;
        return n == 1;
    }

    private int readInt() {
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
                            throw new DataException("Invalid int encoding");
                        }
                    }
                }
            }
        }
        pos += len;
        return (n >>> 1) ^ -(n & 1); // back to two's-complement
    }

    private long readLong() throws IOException {
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
        return (l >>> 1) ^ -(l & 1); // back to two's-complement
    }

    // splitting readLong up makes it faster because of the JVM does more
    // optimizations on small methods
    private long innerLongDecode(long l) {
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
                                throw new DataException("Invalid long encoding");
                            }
                        }
                    }
                }
            }
        }
        pos += len;
        return l;
    }

    private float readFloat() throws IOException {
        int len = 1;
        int n = (buf[pos] & 0xff) | ((buf[pos + len++] & 0xff) << 8)
                | ((buf[pos + len++] & 0xff) << 16) | ((buf[pos + len++] & 0xff) << 24);
        pos += 4;
        return Float.intBitsToFloat(n);
    }

    private double readDouble() throws IOException {
        int len = 1;
        int n1 = (buf[pos] & 0xff) | ((buf[pos + len++] & 0xff) << 8)
                | ((buf[pos + len++] & 0xff) << 16) | ((buf[pos + len++] & 0xff) << 24);
        int n2 = (buf[pos + len++] & 0xff) | ((buf[pos + len++] & 0xff) << 8)
                | ((buf[pos + len++] & 0xff) << 16) | ((buf[pos + len++] & 0xff) << 24);
        pos += 8;
        return Double.longBitsToDouble((((long) n1) & 0xffffffffL)
                | (((long) n2) << 32));
    }

    private ByteString readString() {
        int len = readInt();
        return new ByteString(buf, pos, len, Charsets.UTF_8);
    }

    private ByteBuffer readBytes() {
        int len = readInt();
        return ByteBuffer.wrap(buf, pos, len).slice();
    }

    @Override
    public LazyEvaluatedAvroRecordAdapter put(int index, Object t) {
        throw new UnsupportedOperationException("Lazy evaluated record is not writable!");
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
