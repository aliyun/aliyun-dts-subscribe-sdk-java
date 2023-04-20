package com.taobao.drc.togo.data.schema.impl;

import com.taobao.drc.togo.data.schema.SchemaCache;
import com.taobao.drc.togo.data.schema.SchemaMetaData;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author yangyang
 * @since 17/3/21
 */
public class LocalSchemaCache implements SchemaCache {
    private final ConcurrentMap<SchemaReference, SchemaMetaData> schemaMap = new ConcurrentHashMap<>();

    private SchemaReference createReference(int id, int version) {
        return new SchemaReference(id, version);
    }

    public void put(SchemaMetaData schemaMetaData) {
        schemaMap.putIfAbsent(createReference(schemaMetaData.id(), schemaMetaData.version()), schemaMetaData);
    }

    @Override
    public SchemaMetaData find(int schemaId, int version) {
        return schemaMap.get(createReference(schemaId, version));
    }

    class SchemaReference {
        private final int id;
        private final int version;

        public SchemaReference(int id, int version) {
            this.id = id;
            this.version = version;
        }

        public int getId() {
            return id;
        }

        public int getVersion() {
            return version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SchemaReference that = (SchemaReference) o;
            return id == that.id &&
                    version == that.version;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, version);
        }
    }
}
