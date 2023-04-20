package com.taobao.drc.togo.data.impl.avro;

import com.taobao.drc.togo.data.DataDeserializer;
import com.taobao.drc.togo.data.SchemaAndValue;
import com.taobao.drc.togo.data.SchemaNotFoundException;
import com.taobao.drc.togo.data.SchemafulRecord;
import com.taobao.drc.togo.data.schema.*;
import com.taobao.drc.togo.data.schema.impl.avro.AvroSchemaConverter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.taobao.drc.togo.data.impl.avro.AvroDataSerDeConstants.*;

/**
 * @author yangyang
 * @since 17/3/20
 */
public class AvroDataDeserializer implements DataDeserializer {
    private final boolean lazy;

    public AvroDataDeserializer(boolean lazy) {
        this.lazy = lazy;
    }

    private SchemaCache schemaCache;
    private DecoderFactory decoderFactory = new DecoderFactory();
    private Map<Schema, DatumReader<GenericRecord>> readerMap = new HashMap<>();
    private Map<Schema, org.apache.avro.Schema> avroSchemaMap = new HashMap<>();
    private SchemaConverter<org.apache.avro.Schema> converter = new AvroSchemaConverter();

    public AvroDataDeserializer setSchemaCache(SchemaCache cache) {
        this.schemaCache = cache;
        return this;
    }

    private org.apache.avro.Schema getOrConvertSchema(Schema schema) {
        return avroSchemaMap.computeIfAbsent(schema, s -> converter.toImpl(s));
    }

    private DatumReader getOrCreateReader(Schema schema) {
        return readerMap.computeIfAbsent(schema, s -> new GenericDatumReader<>(getOrConvertSchema(s)));
    }

    private SchemaAndValue wrapValue(int id, int version, SchemafulRecord record) {
        return wrapValue(id, version, record.getSchema(), record);
    }

    private SchemaAndValue wrapValue(int id, int version, Schema schema, Object object) {
        return new SchemaAndValue(id, version, schema, object);
    }

    private Object deserializeAvroValue(byte[] bytes, int offset, int len, Schema schema) {
        BinaryDecoder decoder = decoderFactory.binaryDecoder(bytes, offset, len, null);
        DatumReader reader = getOrCreateReader(schema);
        try {
            Object value = reader.read(null, decoder);
            if (schema.type() == Type.STRUCT) {
                return new AvroGenericRecordAdapter((GenericRecord) value, schema);
            } else {
                return value;
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to deserialize buffer " + Arrays.toString(bytes), e);
        }
    }

    @Override
    public SchemaAndValue deserialize(ByteBuffer byteBuffer) throws SchemaNotFoundException {
        Objects.requireNonNull(schemaCache);

        byteBuffer = byteBuffer.duplicate().order(BYTE_ORDER);
        int magic = byteBuffer.get();
        int attribute = byteBuffer.get();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Illegal magic to deserialize: [" + magic + "], expected: [" + MAGIC + "]");
        }
        boolean withIndex = (attribute & ATTRIBUTE_WITH_FIELD_OFFSET) == 1;

        int schemaId = byteBuffer.getInt();
        int schemaVersion = byteBuffer.getShort();

        SchemaMetaData schemaMetaData = schemaCache.find(schemaId, schemaVersion);
        if (schemaMetaData == null) {
            throw new SchemaNotFoundException(schemaId, schemaVersion);
        }
        Schema schema = schemaMetaData.schema();

        if (withIndex && lazy) {
            return wrapValue(schemaId, schemaVersion, new LazyEvaluatedAvroRecordAdapter(converter, schema, byteBuffer));
        } else {
            int pos = byteBuffer.arrayOffset() + byteBuffer.position();
            int len = byteBuffer.remaining();
            if (withIndex) {
                int fieldIndexSize = AvroDataSerDeConstants.fieldIndexSize(schema.fields().size());
                pos += fieldIndexSize;
                len -= fieldIndexSize;
            }
            return wrapValue(schemaId, schemaVersion, schema, deserializeAvroValue(byteBuffer.array(), pos, len, schema));
        }
    }
}
