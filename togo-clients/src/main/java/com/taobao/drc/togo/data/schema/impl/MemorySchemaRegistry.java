package com.taobao.drc.togo.data.schema.impl;

import com.taobao.drc.togo.data.schema.Schema;
import com.taobao.drc.togo.data.schema.SchemaMetaData;
import com.taobao.drc.togo.data.schema.SchemaRegistry;
import com.taobao.drc.togo.util.concurrent.TogoFuture;
import com.taobao.drc.togo.util.concurrent.TogoFutures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author yangyang
 * @since 17/3/21
 */
public class MemorySchemaRegistry implements SchemaRegistry {
    private static final Logger logger = LoggerFactory.getLogger(MemorySchemaRegistry.class);
    private static final Map<String, Integer> EMPTY_STR_INT_MAP = Collections.unmodifiableMap(new HashMap<>());
    private static final Map<Integer, String> EMPTY_INT_STR_MAP = Collections.unmodifiableMap(new HashMap<>());
    private static final Map<Integer, VersionedSchemaManager> EMPTY_INT_MANAGER_MAP = Collections.unmodifiableMap(new HashMap<>());

    private int maxId = 1;
    private final Map<String, Map<String, Integer>> nameToId = new HashMap<>();
    private final Map<String, Map<Integer, String>> idToName = new HashMap<>();
    private final Map<String, Map<Integer, VersionedSchemaManager>> schemaManagerMap = new HashMap<>();
    private volatile boolean readOnly = false;

    public void setReadOnly(boolean b) {
        this.readOnly = b;
    }

    @Override
    public TogoFuture<SchemaMetaData> register(String topic, String name, Schema schema) {
        Objects.requireNonNull(topic);
        Objects.requireNonNull(name);
        Objects.requireNonNull(schema);
        synchronized (this) {
            Integer id = nameToId.getOrDefault(topic, EMPTY_STR_INT_MAP).get(name);
            if (id == null) {
                if (readOnly) {
                    return TogoFutures.failure(new IllegalStateException("The registry is readonly"));
                }
                id = maxId++;
                nameToId.computeIfAbsent(topic, k -> new HashMap<>()).put(name, id);
                idToName.computeIfAbsent(topic, k -> new HashMap<>()).put(id, name);
            }
            VersionedSchemaManager schemaManager = schemaManagerMap.getOrDefault(topic, EMPTY_INT_MANAGER_MAP).get(id);
            if (schemaManager == null) {
                schemaManager = new VersionedSchemaManager();
                schemaManagerMap.computeIfAbsent(topic, k -> new HashMap<>()).put(id, schemaManager);
            }
            Integer version = schemaManager.getVersionBySchema(schema);
            if (version == null) {
                if (readOnly) {
                    return TogoFutures.failure(new IllegalStateException("The registry is readonly"));
                }
                version = schemaManager.addSchema(schema);
            }
            return TogoFutures.success((SchemaMetaData) new DefaultSchemaMetaData(topic, name, schema, id, version));
        }
    }

    @Override
    public TogoFuture<SchemaMetaData> get(String topic, int schemaId, int version) {
        synchronized (this) {
            String schemaName = idToName.getOrDefault(topic, EMPTY_INT_STR_MAP).get(schemaId);
            VersionedSchemaManager schemaManager = schemaManagerMap.getOrDefault(topic, EMPTY_INT_MANAGER_MAP).get(schemaId);
            if (schemaName == null || schemaManager == null) {
                return TogoFutures.success(null);
            }
            Schema schema = schemaManager.getSchemaByVersion(version);
            if (schema == null) {
                return TogoFutures.success(null);
            }
            return TogoFutures.success((SchemaMetaData) new DefaultSchemaMetaData(topic, schemaName, schema, schemaId, version));
        }
    }

    @Override
    public TogoFuture<Integer> getSchemaIdByName(String topic, String name) {
        synchronized (this) {
            return TogoFutures.success(nameToId.getOrDefault(topic, EMPTY_STR_INT_MAP).get(name));
        }
    }

    @Override
    public TogoFuture<List<SchemaMetaData>> getSchemaListByName(String topic, String name) {
        synchronized (this) {
            Integer schemaId = nameToId.getOrDefault(topic, EMPTY_STR_INT_MAP).get(name);
            if (schemaId == null) {
                return TogoFutures.success((List<SchemaMetaData>) new ArrayList<SchemaMetaData>());
            }
            VersionedSchemaManager manager = schemaManagerMap.getOrDefault(topic, EMPTY_INT_MANAGER_MAP).get(schemaId);
            if (manager == null) {
                return TogoFutures.success((List<SchemaMetaData>) new ArrayList<SchemaMetaData>());
            }
            List<Integer> versions = manager.getSchemaVersions();
            if (versions == null || versions.isEmpty()) {
                return TogoFutures.success((List<SchemaMetaData>) new ArrayList<SchemaMetaData>());
            }
            List<SchemaMetaData> metaDataList = new ArrayList<>();
            for (Integer version : versions) {
                Schema schema = manager.getSchemaByVersion(version);
                if (schema == null) {
                    logger.error("Version [{}] exists for schema [{}][{}], but original schema cannot be retrieved",
                            version, name, schemaId);
                    continue;
                }
                metaDataList.add(new DefaultSchemaMetaData(topic, name, schema, schemaId, version));
            }
            return TogoFutures.success(metaDataList);
        }
    }

    class VersionedSchemaManager {
        private int maxVersion = 1;
        private final Map<Integer, Schema> versionToSchema = new TreeMap<>();
        private final Map<Schema, Integer> schemaToVersion = new HashMap<>();

        public Integer addSchema(Schema schema) {
            Integer version = getVersionBySchema(schema);
            if (version != null) {
                throw new IllegalStateException("Cannot put an schema that already exists");
            }
            version = maxVersion++;
            versionToSchema.put(version, schema);
            schemaToVersion.put(schema, version);
            return version;
        }

        public Integer getVersionBySchema(Schema schema) {
            return schemaToVersion.get(schema);
        }

        public Schema getSchemaByVersion(int version) {
            return versionToSchema.get(version);
        }

        public List<Integer> getSchemaVersions() {
            return new ArrayList<>(versionToSchema.keySet());
        }
    }
}
