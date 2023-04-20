package com.taobao.drc.togo.data.impl.avro;

import com.taobao.drc.togo.data.DataSerializer;
import com.taobao.drc.togo.data.SchemafulRecord;
import com.taobao.drc.togo.data.schema.Field;
import com.taobao.drc.togo.data.schema.SchemaConverter;
import com.taobao.drc.togo.data.schema.SchemaMetaData;
import com.taobao.drc.togo.data.schema.Type;
import com.taobao.drc.togo.data.schema.impl.avro.AvroSchemaConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.taobao.drc.togo.data.impl.avro.AvroDataSerDeConstants.*;

/**
 * @author yangyang
 * @since 17/3/20
 */
public class AvroDataSerializer implements DataSerializer {
    private static final Logger logger = LoggerFactory.getLogger(AvroDataSerializer.class);

    private final Map<com.taobao.drc.togo.data.schema.Schema, DatumWriter> schemaMap = new HashMap<>();
    private final SchemaConverter<Schema> converter = new AvroSchemaConverter();
    private final EncoderFactory encoderFactory = new EncoderFactory();

    private final byte attribute;
    private final boolean withFieldOffset;

    public AvroDataSerializer(boolean withFieldOffset) {
        this.withFieldOffset = withFieldOffset;
        this.attribute = calculateAttribute(withFieldOffset);
    }

    private byte calculateAttribute(boolean withFieldOffset) {
        byte attr = 0;
        if (withFieldOffset) {
            attr |= ATTRIBUTE_WITH_FIELD_OFFSET;
        }
        return attr;
    }

    private DatumWriter getOrCreateAvroWriter(com.taobao.drc.togo.data.schema.Schema origSchema) {
        Objects.requireNonNull(origSchema);
        DatumWriter writer = schemaMap.get(origSchema);
        if (writer == null) {
            Schema schema = converter.toImpl(origSchema);
            writer = new GenericDatumWriter(schema);
            schemaMap.put(origSchema, writer);
        }
        return writer;
    }

    private GenericRecordWrapper wrapRecordToWrite(SchemafulRecord record) {
        return new GenericRecordWrapper(record);
    }

    private void serializeWithOffsets(Object object, OutputStream outputStream, com.taobao.drc.togo.data.schema.Schema schema) throws IOException {
        if (schema.type() == Type.STRUCT) {
            SchemafulRecord record = (SchemafulRecord) object;
            List<Field> schemaFields = record.getSchema().fields();
            int offsetIndexSize = (1 + schemaFields.size()) * SIZE_OF_FIELD_INDEX;
            byte[] indexBytes = new byte[offsetIndexSize];
            ByteBuffer indexBuf = ByteBuffer.wrap(indexBytes).order(BYTE_ORDER);
            indexBuf.putInt(schemaFields.size());
            ByteArrayOutputStream fieldsBuf = new ByteArrayOutputStream();
            for (Field schemaField : schemaFields) {
                indexBuf.putInt(fieldsBuf.size());
                Object item = record.get(schemaField.index());
                serializeWithoutOffsets(item, fieldsBuf, schemaField.schema());
            }
            outputStream.write(indexBytes);
            fieldsBuf.writeTo(outputStream);
            fieldsBuf.close();
        } else {
            serializeWithoutOffsets(object, outputStream, schema);
        }
    }

    private void serializeWithoutOffsets(Object object, OutputStream outputStream, com.taobao.drc.togo.data.schema.Schema schema) throws IOException {
        BinaryEncoder encoder = encoderFactory.directBinaryEncoder(outputStream, null);
        DatumWriter writer = getOrCreateAvroWriter(schema);
        if (object instanceof SchemafulRecord) {
            writer.write(wrapRecordToWrite((SchemafulRecord) object), encoder);
        } else if (object instanceof byte[]) {
            writer.write(ByteBuffer.wrap((byte[]) object), encoder);
        } else {
            writer.write(object, encoder);
        }
    }

    @Override
    public byte[] serialize(Object object, SchemaMetaData schemaMetaData) {
        ByteArrayOutputStream bytesOutStream = new ByteArrayOutputStream();

        try {
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE).order(BYTE_ORDER);
            header.put(MAGIC);
            header.put(attribute);
            header.putInt(schemaMetaData.id());
            header.putShort(schemaMetaData.version().shortValue());
            bytesOutStream.write(header.array());

            if (withFieldOffset) {
                serializeWithOffsets(object, bytesOutStream, schemaMetaData.schema());
            } else {
                serializeWithoutOffsets(object, bytesOutStream, schemaMetaData.schema());
            }
            return bytesOutStream.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize [" + object + "]", e);
        } finally {
            try {
                bytesOutStream.close();
            } catch (IOException e) {
                logger.error("Failed to close backed byte streams", e);
            }
        }
    }

    static class GenericRecordWrapper implements GenericRecord {
        private SchemafulRecord record;

        public GenericRecordWrapper(SchemafulRecord record) {
            this.record = record;
        }

        public SchemafulRecord getRecord() {
            return record;
        }

        public void setRecord(SchemafulRecord record) {
            this.record = record;
        }

        @Override
        public void put(String key, Object v) {
            record.put(key, v);
        }

        @Override
        public Object get(String key) {
            return maybeWrap(record.get(key));
        }

        @Override
        public void put(int i, Object v) {
            record.put(i, v);
        }

        @Override
        public Object get(int i) {
            return maybeWrap(record.get(i));
        }

        @Override
        public Schema getSchema() {
            throw new UnsupportedOperationException();
        }

        private Object maybeWrap(Object object) {
            if (object instanceof SchemafulRecord) {
                return new GenericRecordWrapper((SchemafulRecord) object);
            } else if (object instanceof byte[]) {
                return ByteBuffer.wrap((byte[]) object);
            } else {
                return object;
            }
        }
    }
}
