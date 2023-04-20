package com.taobao.drc.togo.data.schema.impl.avro;

import com.taobao.drc.togo.data.schema.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Objects;

/**
 * @author yangyang
 * @since 17/3/16
 */
public class AvroBasedSchemaSerDe implements SchemaSerializer, SchemaDeserializer {
    public static final byte MAGIC = 0;
    public static final int MAGIC_SIZE = 1;
    public static final int SCHEMA_LENGTH_OFFSET = MAGIC_SIZE;
    public static final int SCHEMA_LENGTH_SIZE = 4;
    public static final int BODY_OFFSET = SCHEMA_LENGTH_OFFSET + SCHEMA_LENGTH_SIZE;
    public static final ByteOrder DEFAULT_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private static final Charset charset = Charset.forName("utf8");
    private final SchemaConverter<org.apache.avro.Schema> converter = new AvroSchemaConverter();

    private org.apache.avro.Schema.Parser getParser() {
        return new org.apache.avro.Schema.Parser();
    }

    @Override
    public Schema deserialize(ByteBuffer inBuffer) {
        Objects.requireNonNull(inBuffer);
        if (inBuffer.remaining() == 0) {
            throw new SchemaException("Illegal schema format: empty bytes");
        }
        ByteBuffer buf = inBuffer.duplicate().order(DEFAULT_BYTE_ORDER);
        byte magic = buf.get();
        if (magic != MAGIC) {
            throw new SchemaException("Illegal magic [" + magic + "], required [" + MAGIC + "]");
        }
        int size = buf.getInt();
        if (size <= 0 || size > buf.remaining()) {
            throw new SchemaException("Invalid schema size [" + size + "]");
        }
        byte[] bytes = new byte[size];
        buf.get(bytes);
        org.apache.avro.Schema schema = getParser().parse(new String(bytes, charset));
        return converter.fromImpl(schema);
    }

    @Override
    public byte[] serialize(Schema schema) {
        org.apache.avro.Schema avroSchema = converter.toImpl(schema);
        byte[] schemaBytes = avroSchema.toString().getBytes(charset);
        byte[] serializedBytes = new byte[schemaBytes.length + BODY_OFFSET];
        serializedBytes[0] = MAGIC;
        ByteBuffer.wrap(serializedBytes).order(DEFAULT_BYTE_ORDER).putInt(SCHEMA_LENGTH_OFFSET, schemaBytes.length);
        System.arraycopy(schemaBytes, 0, serializedBytes, BODY_OFFSET, schemaBytes.length);
        return serializedBytes;
    }
}
