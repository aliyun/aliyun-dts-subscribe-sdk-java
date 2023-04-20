package com.taobao.drc.togo.data.schema.impl.avro;

import com.taobao.drc.togo.data.schema.Field;
import com.taobao.drc.togo.data.schema.SchemaConverter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.Map;
import java.util.Objects;

/**
 * @author yangyang
 * @since 17/3/16
 */
public class AvroSchemaConverter implements SchemaConverter<Schema> {
    public static final String DEFAULT_SCHEMA_NAME = "AvroDefault";
    public static final String PROP_LOGICAL_TYPE = "togo.logicalType";

    @Override
    public Schema toImpl(com.taobao.drc.togo.data.schema.Schema schema) {
        Objects.requireNonNull(schema);
        Schema buildSchema = null;
        switch (schema.type()) {
            case BOOLEAN:
                buildSchema = SchemaBuilder.builder().booleanType();
                break;
            case INT32:
                buildSchema = SchemaBuilder.builder().intType();
                break;
            case INT64:
                buildSchema = SchemaBuilder.builder().longType();
                break;
            case FLOAT32:
                buildSchema = SchemaBuilder.builder().floatType();
                break;
            case FLOAT64:
                buildSchema = SchemaBuilder.builder().doubleType();
                break;
            case BYTES:
                buildSchema = SchemaBuilder.builder().bytesType();
                break;
            case STRING:
                buildSchema = SchemaBuilder.builder().stringType();
                break;
            case ARRAY:
                buildSchema = SchemaBuilder.builder().array().items(toImpl(schema.valueSchema()));
                break;
            case MAP:
                buildSchema = SchemaBuilder.builder().map().values(toImpl(schema.valueSchema()));
                break;
            case STRUCT: {
                SchemaBuilder.FieldAssembler<Schema> assembler = SchemaBuilder.builder()
                        .record(schema.name() != null ? schema.name() : DEFAULT_SCHEMA_NAME)
                        .doc(schema.doc())
                        .fields();
                for (Field field : schema.fields()) {
                    SchemaBuilder.GenericDefault<Schema> builder = assembler
                            .name(field.name())
                            .doc(field.schema().doc())
                            .type(toImpl(field.schema()));
                    if (field.schema().defaultValue() == null) {
                        assembler = builder.noDefault();
                    } else {
                        assembler = builder.withDefault(field.schema().defaultValue());
                    }
                }
                buildSchema = assembler.endRecord();
                break;
            }
            default:
                throw new IllegalArgumentException("Invalid schema type [" + schema.type() + "] in schema [" + schema + "]");
        }
        if (schema.parameters() != null) {
            for (Map.Entry<String, String> stringStringEntry : schema.parameters().entrySet()) {
                buildSchema.addProp(stringStringEntry.getKey(), stringStringEntry.getValue());
            }
        }
        if (schema.logicalType() != null) {
            buildSchema.addProp(PROP_LOGICAL_TYPE, buildSchema.getLogicalType());
        }
        return buildSchema;
    }

    private com.taobao.drc.togo.data.schema.SchemaBuilder fromAsBuilder(Schema schema) {
        Objects.requireNonNull(schema);

        com.taobao.drc.togo.data.schema.SchemaBuilder builder = null;
        switch (schema.getType()) {
            case BOOLEAN:
                builder = com.taobao.drc.togo.data.schema.SchemaBuilder.bool();
                break;
            case INT:
                builder = com.taobao.drc.togo.data.schema.SchemaBuilder.int32();
                break;
            case LONG:
                builder = com.taobao.drc.togo.data.schema.SchemaBuilder.int64();
                break;
            case FLOAT:
                builder = com.taobao.drc.togo.data.schema.SchemaBuilder.float32();
                break;
            case DOUBLE:
                builder = com.taobao.drc.togo.data.schema.SchemaBuilder.float64();
                break;
            case BYTES:
                builder = com.taobao.drc.togo.data.schema.SchemaBuilder.bytes();
                break;
            case STRING:
                builder = com.taobao.drc.togo.data.schema.SchemaBuilder.string();
                break;
            case ARRAY:
                builder = com.taobao.drc.togo.data.schema.SchemaBuilder.array(fromImpl(schema.getValueType()));
                break;
            case MAP:
                builder = com.taobao.drc.togo.data.schema.SchemaBuilder.map(fromImpl(schema.getValueType()));
                break;
            case RECORD: {
                builder = com.taobao.drc.togo.data.schema.SchemaBuilder.struct()
                        .name(schema.getName())
                        .doc(schema.getDoc());
                for (Schema.Field field : schema.getFields()) {
                    com.taobao.drc.togo.data.schema.SchemaBuilder fieldBuilder = fromAsBuilder(field.schema()).doc(field.doc());
                    if (field.defaultVal() != null) {
                        fieldBuilder.optional().defaultValue(field.defaultVal());
                    } else {
                        fieldBuilder.required();
                    }
                    builder.field(field.name(), fieldBuilder);
                }
                break;
            }
            default:
                throw new IllegalArgumentException("Invalid schema type [" + schema.getType() + "] in schema [" + schema + "]");
        }
        for (Map.Entry<String, Object> entry : schema.getObjectProps().entrySet()) {
            if (PROP_LOGICAL_TYPE.equals(entry.getKey())) {
                builder.logical(entry.getValue().toString());
            } else {
                builder.parameter(entry.getKey(), entry.getValue().toString());
            }
        }
        return builder;
    }

    @Override
    public com.taobao.drc.togo.data.schema.Schema fromImpl(Schema schema) {
        return fromAsBuilder(schema).build();
    }
}
