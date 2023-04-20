package com.taobao.drc.togo.data;

import com.taobao.drc.togo.data.impl.DefaultSchemafulRecord;
import com.taobao.drc.togo.data.impl.IntegratedDataDeserializer;
import com.taobao.drc.togo.data.impl.avro.AvroDataSerializer;
import com.taobao.drc.togo.data.schema.*;
import com.taobao.drc.togo.data.schema.impl.avro.AvroBasedSchemaSerDe;

import java.util.Objects;

/**
 * @author yangyang
 * @since 17/3/21
 */
public class Data {
    private static final Data data = new Data();
    private static final AvroBasedSchemaSerDe schemaSerde = new AvroBasedSchemaSerDe();

    public static Data get() {
        return data;
    }

    public SchemafulRecord newRecord(Schema schema) {
        return new DefaultSchemafulRecord(schema);
    }

    public SchemafulRecord copyRecord(SchemafulRecord record) {
        Objects.requireNonNull(record);
        SchemafulRecord copied = newRecord(record.getSchema());
        for (Field field : record.getSchema().fields()) {
            switch (field.schema().type()) {
                case STRUCT:
                    copied.put(field.index(), copyRecord((SchemafulRecord) record.get(field.index())));
                    break;
                default:
                    copied.put(field.index(), record.get(field.index()));
                    break;
            }
        }
        return copied;
    }

    public DataDeserializer deserializer(SchemaCache cache) {
        return deserializer(cache, false);
    }

    public DataDeserializer deserializer(SchemaCache cache, boolean lazy) {
        return new IntegratedDataDeserializer(lazy).setSchemaCache(cache);
    }

    public DataSerializer serializer(boolean withFieldIndex) {
        return new DataSerializerBuilder().withFieldIndex(withFieldIndex).build();
    }

    public SchemaSerializer schemaSerializer() {
        return schemaSerde;
    }

    public SchemaDeserializer schemaDeserializer() {
        return schemaSerde;
    }

    public static class DataSerializerBuilder {
        private boolean withFieldIndex = true;

        DataSerializerBuilder() {
        }

        public DataSerializerBuilder withFieldIndex(boolean b) {
            this.withFieldIndex = b;
            return this;
        }

        public DataSerializer build() {
            return new AvroDataSerializer(withFieldIndex);
        }
    }
}
