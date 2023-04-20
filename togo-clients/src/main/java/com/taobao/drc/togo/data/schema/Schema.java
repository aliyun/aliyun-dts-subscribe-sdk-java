package com.taobao.drc.togo.data.schema;

import java.util.List;
import java.util.Map;

/**
 * @author yangyang
 * @since 17/3/8
 */
public interface Schema {
    /**
     * @return the type of the schema.
     */
    Type type();

    /**
     * @return the name of the schema.
     */
    String name();

    /**
     * The human readable description of the schema.
     *
     * @return schema description.
     */
    String doc();

    /**
     * (1) If the type of the schema is ARRAY, this is the schema of the elements.
     * (2) If the type of the schema is MAP, this is the schema of the values of the map.
     * (3) Otherwise, this should be null.
     *
     * @return schema of the value.
     */
    Schema valueSchema();

    /**
     * @return the name of the logical type.
     */
    String logicalType();

    /**
     * Get the parameters of the schema. The parameters can be used for complex schemas and logical types.
     *
     * @return the parameters of the schema.
     */
    Map<String, String> parameters();

    /**
     * Get the field of the struct by name. Throws a SchemaException if the schema is not a struct.
     *
     * @param name The name of the field.
     * @return the field matches the name, or null if the schema has no field with the given name.
     */
    Field field(String name);

    /**
     * @return the fields of the schema.
     */
    List<Field> fields();

    /**
     * @return true if the field is optional, false otherwise.
     */
    boolean isOptional();

    /**
     * @return the default value for this schema.
     */
    Object defaultValue();
}
