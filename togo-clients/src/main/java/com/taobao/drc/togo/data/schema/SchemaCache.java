package com.taobao.drc.togo.data.schema;

/**
 * @author yangyang
 * @since 17/3/8
 */
public interface SchemaCache {
    /**
     * Get schema by (id, version) locally. This class is typically used to find the schema in deserializers,
     * the cache should not block and returns null immediately when no schema is found locally.
     *
     * @param schemaId the id of the schema.
     * @param version the version of the schema.
     * @return the schema with given (schemaId, version), or null if no schema found.
     */
    SchemaMetaData find(int schemaId, int version);
}
