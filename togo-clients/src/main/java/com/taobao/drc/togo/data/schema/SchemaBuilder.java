package com.taobao.drc.togo.data.schema;

import com.taobao.drc.togo.data.impl.DefaultSchemafulRecord;
import com.taobao.drc.togo.data.schema.impl.DefaultSchema;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author yangyang
 * @since 17/3/8
 */
public class SchemaBuilder implements Schema {
    private static final String NAME_FIELD = "name";
    private static final String DOC_FIELD = "doc";
    private static final String TYPE_FIELD = "type";
    private static final String OPTIONAL_FIELD = "optional";
    private static final String DEFAULT_FIELD = "default";

    private static final Map<Type, List<Class>> SCHEMA_TYPE_CLASSES = new EnumMap<>(Type.class);

    static {
        SCHEMA_TYPE_CLASSES.put(Type.INT32, Collections.singletonList((Class) Integer.class));
        SCHEMA_TYPE_CLASSES.put(Type.INT64, Collections.singletonList((Class) Long.class));
        SCHEMA_TYPE_CLASSES.put(Type.FLOAT32, Collections.singletonList((Class) Float.class));
        SCHEMA_TYPE_CLASSES.put(Type.FLOAT64, Collections.singletonList((Class) Double.class));
        SCHEMA_TYPE_CLASSES.put(Type.BOOLEAN, Collections.singletonList((Class) Boolean.class));
        SCHEMA_TYPE_CLASSES.put(Type.STRING, Collections.singletonList((Class) String.class));
        // Bytes are special and have 2 representations. byte[] causes problems because it doesn't handle equals() and
        // hashCode() like we want objects to, so we support both byte[] and ByteBuffer. Using plain byte[] can cause
        // those methods to fail, so ByteBuffers are recommended
        SCHEMA_TYPE_CLASSES.put(Type.BYTES, Arrays.asList((Class) byte[].class, (Class) ByteBuffer.class));
        SCHEMA_TYPE_CLASSES.put(Type.ARRAY, Collections.singletonList((Class) List.class));
        SCHEMA_TYPE_CLASSES.put(Type.MAP, Collections.singletonList((Class) Map.class));
        SCHEMA_TYPE_CLASSES.put(Type.STRUCT, Collections.singletonList((Class) DefaultSchemafulRecord.class));
    }

    private final Type type;
    private String name = null;
    private String doc = null;
    private String logicalType = null;
    private Boolean optional = null;
    private Object defaultValue = null;
    private Map<String, String> parameters = null;
    private Schema valueSchema = null;
    private List<Field> fields = null;
    private List<String> primaryKeys = null;

    public static void validateValue(Schema schema, Object value) {
        if (value == null) {
            if (!schema.isOptional()) {
                throw new SchemaBuilderException("Invalid value: null used for required field");
            } else {
                return;
            }
        }

        List<Class> expectedClasses = SCHEMA_TYPE_CLASSES.get(schema.type());

        if (expectedClasses == null) {
            throw new SchemaBuilderException("Invalid Java object for schema type " + schema.type() + ": " + value.getClass());
        }

        boolean foundMatch = false;
        for (int i = 0; i < expectedClasses.size(); i++) {
            Class<?> expectedClass = expectedClasses.get(i);
            if (expectedClass.isInstance(value)) {
                foundMatch = true;
                break;
            }
        }
        if (!foundMatch) {
            throw new SchemaBuilderException("Invalid Java object for schema type " + schema.type() + ": " + value.getClass());
        }

        switch (schema.type()) {
            case STRUCT:
                DefaultSchemafulRecord struct = (DefaultSchemafulRecord) value;
                if (!struct.getSchema().equals(schema)) {
                    throw new SchemaBuilderException("Struct schemas do not match.");
                }
                struct.validate();
                break;
            case ARRAY:
                List<?> array = (List<?>) value;
                for (Object entry : array) {
                    validateValue(schema.valueSchema(), entry);
                }
                break;
            case MAP:
                Map<?, ?> map = (Map<?, ?>) value;
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    validateValue(schema.valueSchema(), entry.getValue());
                }
                break;
        }
    }

    /**
     * Create a SchemaBuilder for the specified type.
     * <p>
     * Usually it will be simpler to use one of the variants like {@link #string()} or {@link #struct()}, but this form
     * can be useful when generating schemas dynamically.
     *
     * @param type the schema type
     * @return a new SchemaBuilder
     */
    public static SchemaBuilder type(Type type) {
        return new SchemaBuilder(type);
    }

    public static SchemaBuilder bool() {
        return new SchemaBuilder(Type.BOOLEAN);
    }

    public static SchemaBuilder int32() {
        return new SchemaBuilder(Type.INT32);
    }

    public static SchemaBuilder int64() {
        return new SchemaBuilder(Type.INT64);
    }

    public static SchemaBuilder float32() {
        return new SchemaBuilder(Type.FLOAT32);
    }

    public static SchemaBuilder float64() {
        return new SchemaBuilder(Type.FLOAT64);
    }

    public static SchemaBuilder string() {
        return new SchemaBuilder(Type.STRING);
    }

    public static SchemaBuilder bytes() {
        return new SchemaBuilder(Type.BYTES);
    }

    public static SchemaBuilder struct() {
        return new SchemaBuilder(Type.STRUCT);
    }

    public static SchemaBuilder array(Schema valueSchema) {
        SchemaBuilder builder = new SchemaBuilder(Type.ARRAY);
        builder.valueSchema = valueSchema;
        return builder;
    }

    public static SchemaBuilder map(Schema valueSchema) {
        SchemaBuilder builder = new SchemaBuilder(Type.MAP);
        builder.valueSchema = valueSchema;
        return builder;
    }

    private SchemaBuilder(Type type) {
        this.type = type;
        if (type == Type.STRUCT) {
            fields = new ArrayList<>();
        }
    }

    @Override
    public Type type() {
        return type;
    }

    /**
     * Set the name of this schema.
     *
     * @param name the schema name
     * @return the SchemaBuilder
     */
    public SchemaBuilder name(String name) {
        checkNull(NAME_FIELD, this.name);
        this.name = name;
        return this;
    }

    @Override
    public String name() {
        return name;
    }

    /**
     * Set the documentation for this schema.
     *
     * @param doc the documentation
     * @return the SchemaBuilder
     */
    public SchemaBuilder doc(String doc) {
        checkNull(DOC_FIELD, this.doc);
        this.doc = doc;
        return this;
    }

    @Override
    public String doc() {
        return doc;
    }

    /**
     * Set a schema parameter.
     *
     * @param propertyName  name of the schema property to define
     * @param propertyValue value of the schema property to define, as a String
     * @return the SchemaBuilder
     */
    public SchemaBuilder parameter(String propertyName, String propertyValue) {
        // Preserve order of insertion with a LinkedHashMap. This isn't strictly necessary, but is nice if logical types
        // can print their properties in a consistent order.
        if (parameters == null) {
            parameters = new LinkedHashMap<>();
        }
        parameters.put(propertyName, propertyValue);
        return this;
    }

    /**
     * Set schema parameters. This operation is additive; it does not remove existing parameters that do not appear in
     * the set of properties pass to this method.
     *
     * @param props Map of properties to set
     * @return the SchemaBuilder
     */
    public SchemaBuilder parameters(Map<String, String> props) {
        // Avoid creating an empty set of properties so we never have an empty map
        if (props.isEmpty()) {
            return this;
        }
        if (parameters == null) {
            parameters = new LinkedHashMap<>();
        }
        parameters.putAll(props);
        return this;
    }

    /**
     * Add a field to this struct schema. Throws a SchemaBuilderException if this is not a struct schema.
     *
     * @param fieldName   the name of the field to add
     * @param fieldSchema the Schema for the field's value
     * @return the SchemaBuilder
     */
    public SchemaBuilder field(String fieldName, Schema fieldSchema) {
        if (type != Type.STRUCT) {
            throw new SchemaBuilderException("Cannot create fields on type " + type);
        }
        int fieldIndex = fields.size();
        fields.add(new Field(fieldName, fieldIndex, fieldSchema));
        return this;
    }

    public SchemaBuilder logical(String logicalType) {
        this.logicalType = logicalType;
        return this;
    }

    @Override
    public String logicalType() {
        return logicalType;
    }

    @Override
    public Map<String, String> parameters() {
        return parameters;
    }

    public Field field(String fieldName) {
        if (type != Type.STRUCT) {
            throw new SchemaException("Cannot look up fields on non-struct type");
        }
        for (Field field : fields) {
            if (field.name().equals(fieldName)) {
                return field;
            }
        }
        return null;
    }

    /**
     * Get the list of fields for this Schema. Throws a DataException if this schema is not a struct.
     *
     * @return the list of fields for this Schema
     */
    public List<Field> fields() {
        if (type != Type.STRUCT) {
            throw new SchemaException("Cannot list fields on non-struct type");
        }
        return fields;
    }

    public SchemaBuilder required() {
        checkNull(OPTIONAL_FIELD, optional);
        this.optional = false;
        return this;
    }

    public SchemaBuilder optional() {
        checkNull(OPTIONAL_FIELD, optional);
        this.optional = true;
        return this;
    }

    @Override
    public boolean isOptional() {
        return optional == Boolean.TRUE;
    }

    public SchemaBuilder defaultValue(Object value) {
        checkNull(DEFAULT_FIELD, defaultValue);
        checkNotNull(TYPE_FIELD, type, DEFAULT_FIELD);
        validateValue(this, value);
        this.defaultValue = value;
        return this;
    }

    @Override
    public Object defaultValue() {
        return defaultValue;
    }

    @Override
    public Schema valueSchema() {
        return valueSchema;
    }

    private static void checkNull(String fieldName, Object val) {
        if (val != null) {
            throw new SchemaBuilderException("Invalid SchemaBuilder call: " + fieldName + " has already been set.");
        }
    }

    private static void checkNotNull(String fieldName, Object val, String fieldToSet) {
        if (val == null) {
            throw new SchemaBuilderException("Invalid SchemaBuilder call: " + fieldName + " must be specified to set " + fieldToSet);
        }
    }

    private Schema maybeBuildSchema(Schema schema) {
        if (schema instanceof SchemaBuilder) {
            return ((SchemaBuilder) schema).build();
        } else {
            return schema;
        }
    }

    private List<Field> maybeBuildFieldSchema() {
        if (fields == null) {
            return null;
        } else {
            List<Field> ret = new ArrayList<>();
            for (Field field : fields) {
                ret.add(new Field(field.name(), field.index(), maybeBuildSchema(field.schema())));
            }
            return Collections.unmodifiableList(ret);
        }
    }

    /**
     * Build the Schema using the current settings
     *
     * @return the {@link Schema}
     */
    public Schema build() {
        return new DefaultSchema(name, doc, type,
                maybeBuildSchema(valueSchema),
                maybeBuildFieldSchema(),
                logicalType, parameters == null ? null : Collections.unmodifiableMap(parameters),
                isOptional(), defaultValue());
    }

    @Override
    public String toString() {
        return "SchemaBuilder{" +
                "type=" + type +
                ", name='" + name + '\'' +
                ", doc='" + doc + '\'' +
                ", logicalType='" + logicalType + '\'' +
                ", optional=" + optional +
                ", defaultValue=" + defaultValue +
                ", parameters=" + parameters +
                ", valueSchema=" + valueSchema +
                ", fields=" + fields +
                ", primaryKeys=" + primaryKeys +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaBuilder that = (SchemaBuilder) o;
        return type == that.type &&
                Objects.equals(name, that.name) &&
                Objects.equals(doc, that.doc) &&
                Objects.equals(logicalType, that.logicalType) &&
                Objects.equals(optional, that.optional) &&
                Objects.equals(defaultValue, that.defaultValue) &&
                Objects.equals(parameters, that.parameters) &&
                Objects.equals(valueSchema, that.valueSchema) &&
                Objects.equals(fields, that.fields) &&
                Objects.equals(primaryKeys, that.primaryKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name, doc, logicalType, optional, defaultValue, parameters, valueSchema, fields, primaryKeys);
    }

}
