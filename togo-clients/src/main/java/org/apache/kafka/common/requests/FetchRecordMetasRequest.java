package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.meta.RecordIndex;
import org.apache.kafka.common.record.meta.RecordSchema;
import org.apache.kafka.common.record.meta.RecordTag;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.apache.kafka.common.protocol.types.Type.*;

public class FetchRecordMetasRequest extends AbstractRequest {

    private static final String FETCH_TOPIC_KEY_NAME = "topic";
    private static final String FETCH_RECORD_SCHEMAS_KEY_NAME = "schemas";
    private static final String FETCH_RECORD_INDEXES_KEY_NAME = "indexes";
    private static final String FETCH_RECORD_TAGS_KEY_NAME = "tags";

    private static final String FETCH_META_ALL_KEY_NAME = "fetch_all";
    private static final String FETCH_META_NAMES_KEY_NAME = "names";
    private static final String FETCH_SCHEMA_ID_VERSIONS_KEY_NAME = "schema_id_versions";

    private static final String SCHEMA_ID_KEY_NAME = "id";
    private static final String SCHEMA_VERSION_KEY_NAME = "version";

    // api FetchRecordMetas
    public static final Schema FETCH_SCHEMA_ENTRY_V0 = new Schema(
            new Field("id", INT32, "The schema id to fetch"),
            new Field("version", INT16, "The schema version to fetch")
    );

    public static final Schema FETCH_META_ENTRIES_V0 = new Schema(
            new Field("fetch_all", BOOLEAN, "Wether to fetch all metas"),
            new Field("names", ArrayOf.nullable(STRING), "The names of meta are to be fetched"),
            new Field("schema_id_versions", ArrayOf.nullable(FETCH_SCHEMA_ENTRY_V0), "Schema exact infos")
    );

    public static final Schema FETCH_RECORD_SCHEMA_ENTRIES_V0 = FETCH_META_ENTRIES_V0;

    public static final Schema FETCH_RECORD_INDEX_ENTRIES_V0 = FETCH_META_ENTRIES_V0;

    public static final Schema FETCH_RECORD_TAG_ENTRIES_V0 = FETCH_META_ENTRIES_V0;

    public static final Schema FETCH_RECORD_META_REQUEST_V0 = new Schema(
            new Field("topic", STRING, "The name of topic"),
            new Field("schemas", FETCH_RECORD_SCHEMA_ENTRIES_V0, "The schemas to be fetched"),
            new Field("indexes", FETCH_RECORD_INDEX_ENTRIES_V0),
            new Field("tags", FETCH_RECORD_TAG_ENTRIES_V0)
    );


    public static Schema[] schemaVersions() {
        return  new Schema[]{FETCH_RECORD_META_REQUEST_V0};
    }

    public static class SchemaIDVersion {
        public final int id;
        public final short version;

        public SchemaIDVersion(int id, short version) {
            this.id = id;
            this.version = version;
        }

        @Override
        public String toString() {
            return "(type=SchemaIDVersion, id=" + id + ", version=" + version + ")";
        }
    }

    public static class RecordMetasToFetch {
        public final boolean isFetchAll;
        public final List<String> metaNames;
        public final Set<SchemaIDVersion> schemaIDVersions;

        public RecordMetasToFetch() {
            this(true);
        }

        public RecordMetasToFetch(List<String> metaNames) {
            this.isFetchAll = false;
            this.metaNames = metaNames;
            this.schemaIDVersions = null;
        }

        public RecordMetasToFetch(boolean isFetchAll) {
            this.isFetchAll = isFetchAll;
            this.metaNames = null;
            this.schemaIDVersions = null;
        }

        public RecordMetasToFetch(Set<SchemaIDVersion> schemaIDVersions) {
            this.isFetchAll = false;
            this.metaNames = null;
            this.schemaIDVersions = schemaIDVersions;
        }

        public boolean isValid() {
            return isFetchAll || null != metaNames || null != schemaIDVersions;
        }

        public static RecordMetasToFetch invalidInstance() {
            return new RecordMetasToFetch(false);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=RecordMetasToFetch").
                    append(", isFetchAll=").append(isFetchAll).
                    append(", metaNames=").append(Utils.mkString(metaNames, ",")).
                    append(", schemaIDVersions=").append(Utils.mkString(schemaIDVersions, ",")).
                    append(")");
            return bld.toString();
        }
    }

    public static class Builder extends AbstractRequest.Builder<FetchRecordMetasRequest> {
        private final String topic;
        private final RecordMetasToFetch schemasToFetch;
        private final RecordMetasToFetch indexesToFetch;
        private final RecordMetasToFetch tagsToFetch;

        public static Builder allSchemas(String topic) {
            return new Builder(topic, new RecordMetasToFetch(), null, null);
        }

        public static Builder allIndexes(String topic) {
            return new Builder(topic, null, new RecordMetasToFetch(), null);
        }

        public static Builder allTags(String topic) {
            return new Builder(topic, null, null, new RecordMetasToFetch());
        }

        public static Builder specifySchemas(String topic, Set<SchemaIDVersion> schemaIDVersions) {
            return new Builder(topic, new RecordMetasToFetch(schemaIDVersions), null, null);
        }

        public static Builder specifySchemas(String topic, List<String> schemaNames) {
            return new Builder(topic, new RecordMetasToFetch(schemaNames), null, null);
        }

        public static Builder specifyIndexes(String topic, List<String> indexNames) {
            return new Builder(topic, null, new RecordMetasToFetch(indexNames), null);
        }

        public static Builder specifyTags(String topic, List<String> tagNames) {
            return new Builder(topic, null, null, new RecordMetasToFetch(tagNames));
        }

        public Builder(String topic, RecordMetasToFetch schemasToFetch, RecordMetasToFetch indexesToFetch,
                       RecordMetasToFetch tagsToFetch) {
            super(ApiKeys.FETCH_RECORD_METAS);
            this.topic = topic;
            this.schemasToFetch = (null == schemasToFetch ? RecordMetasToFetch.invalidInstance() : schemasToFetch);
            this.indexesToFetch = (null == indexesToFetch ? RecordMetasToFetch.invalidInstance() : indexesToFetch);
            this.tagsToFetch = (null == tagsToFetch ? RecordMetasToFetch.invalidInstance() : tagsToFetch);
        }

        public String topic() {
            return this.topic;
        }

        public RecordMetasToFetch schemas() {
            return this.schemasToFetch;
        }

        public RecordMetasToFetch indexes() {
            return this.indexesToFetch;
        }

        public RecordMetasToFetch tags() {
            return this.tagsToFetch;
        }

        @Override
        public FetchRecordMetasRequest build(short version) {
            return new FetchRecordMetasRequest(topic, schemasToFetch, indexesToFetch, tagsToFetch, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=FetchRecordMetas").
                    append(", topics=").append(topic).
                    append(", schemas=").append(schemasToFetch).
                    append(", indexes=").append(indexesToFetch).
                    append(", tags=").append(tagsToFetch).
                    append(")");
            return bld.toString();
        }
    }

    private final String topic;
    private final RecordMetasToFetch schemasToFetch;
    private final RecordMetasToFetch indexesToFetch;
    private final RecordMetasToFetch tagsToFetch;

    private void setFetchMetaStruct(Struct struct, String keyName, RecordMetasToFetch metasToFetch) {
        if (metasToFetch != null) {
            final Struct fetchMetaStruct = struct.instance(keyName);
            fetchMetaStruct.set(FETCH_META_ALL_KEY_NAME, metasToFetch.isFetchAll);
            if (null != metasToFetch.metaNames) {
                fetchMetaStruct.set(FETCH_META_NAMES_KEY_NAME, metasToFetch.metaNames.toArray());
            }
            if (null != metasToFetch.schemaIDVersions) {
                List<Struct> idVersionStructs = new ArrayList<>(metasToFetch.schemaIDVersions.size());
                metasToFetch.schemaIDVersions.forEach(idVersion -> {
                    Struct idVersionStruct = fetchMetaStruct.instance(FETCH_SCHEMA_ID_VERSIONS_KEY_NAME);
                    idVersionStruct.set(SCHEMA_ID_KEY_NAME, idVersion.id);
                    idVersionStruct.set(SCHEMA_VERSION_KEY_NAME, idVersion.version);
                    idVersionStructs.add(idVersionStruct);
                });
                fetchMetaStruct.set(FETCH_SCHEMA_ID_VERSIONS_KEY_NAME, idVersionStructs.toArray());
            }
            struct.set(keyName, fetchMetaStruct);
        }
    }

    private List<String> toStringList(Object[] objects) {
        List<String> ret = null;
        if (null != objects) {
            ret = new ArrayList<>(objects.length);
            for (Object object : objects) {
                String item = (String) object;
                ret.add(item);
            }
        }
        return ret;
    }

    private Set<SchemaIDVersion> toSchemaIDVersionSet(Object[] objects) {
        Set<SchemaIDVersion> ret = null;
        if (null != objects) {
            ret = new HashSet<>();
            for (Object object : objects) {
                Struct idVersionStruct = (Struct)object;
                SchemaIDVersion idVersion = new SchemaIDVersion(idVersionStruct.getInt(SCHEMA_ID_KEY_NAME),
                        idVersionStruct.getShort(SCHEMA_VERSION_KEY_NAME));
                ret.add(idVersion);
            }
        }
        return ret;
    }

    private RecordMetasToFetch extractFetchMetaStruct(String keyName, Struct struct) {
        Struct fetchMetaStruct = struct.getStruct(keyName);

        // null for keyName, just return null
        if (null == fetchMetaStruct) {
            return null;
        }

        //  fetch all
        if (fetchMetaStruct.getBoolean(FETCH_META_ALL_KEY_NAME)) {
            return new RecordMetasToFetch();
        }

        // 1st check names, 2nd check schema id-versions
        List<String> names = toStringList(fetchMetaStruct.getArray(FETCH_META_NAMES_KEY_NAME));
        Set<SchemaIDVersion> idVersions = toSchemaIDVersionSet(fetchMetaStruct.getArray(FETCH_SCHEMA_ID_VERSIONS_KEY_NAME));
        if (null != names) {
            return new RecordMetasToFetch(names);
        }
        return new RecordMetasToFetch(idVersions);
    }

    public FetchRecordMetasRequest(String topic, RecordMetasToFetch schemas, RecordMetasToFetch indexes,
                                   RecordMetasToFetch tags, short version) {
        super( version);
        this.topic = topic;
        this.schemasToFetch = schemas;
        this.indexesToFetch = indexes;
        this.tagsToFetch = tags;
    }

    public FetchRecordMetasRequest(Struct struct, short version) {
        super(version);
        topic = struct.getString(FETCH_TOPIC_KEY_NAME);
        schemasToFetch = extractFetchMetaStruct(FETCH_RECORD_SCHEMAS_KEY_NAME, struct);
        indexesToFetch = extractFetchMetaStruct(FETCH_RECORD_INDEXES_KEY_NAME, struct);
        tagsToFetch = extractFetchMetaStruct(FETCH_RECORD_TAGS_KEY_NAME, struct);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.FETCH_RECORD_METAS.requestSchema(version()));

        struct.set(FETCH_TOPIC_KEY_NAME, topic);
        setFetchMetaStruct(struct, FETCH_RECORD_SCHEMAS_KEY_NAME, schemasToFetch);
        setFetchMetaStruct(struct, FETCH_RECORD_INDEXES_KEY_NAME, indexesToFetch);
        setFetchMetaStruct(struct, FETCH_RECORD_TAGS_KEY_NAME, tagsToFetch);
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        Errors error = Errors.forException(e);
        List<RecordSchema> schemas = null;
        List<RecordIndex> indexes = null;
        List<RecordTag> tags = null;

        Function<String, RecordSchema> allocRecordSchema = name -> {
            RecordSchema recordSchema = new RecordSchema(name);
            recordSchema.setError(error);
            return recordSchema;
        };

        Function<String, RecordIndex> allocRecordIndex = name -> {
            RecordIndex recordIndex = new RecordIndex(name);
            recordIndex.setError(error);
            return recordIndex;
        };

        Function<String, RecordTag> allocRecordTag = name -> {
            RecordTag recordTag = new RecordTag(name);
            recordTag.setError(error);
            return recordTag;
        };

        // compose schemas response
        if (null != schemasToFetch && schemasToFetch.isValid()) {
            final List<RecordSchema> finalSchemas = new ArrayList<>();
            if (schemasToFetch.isFetchAll) {
                finalSchemas.add(allocRecordSchema.apply(""));
            } else if (null != schemasToFetch.metaNames) {
                schemasToFetch.metaNames.forEach(
                        name -> finalSchemas.add(allocRecordSchema.apply(name)));
            } else {
                schemasToFetch.schemaIDVersions.forEach(idVersion -> {
                    RecordSchema recordSchema = allocRecordSchema.apply("");
                    recordSchema.id = idVersion.id;
                    recordSchema.version = idVersion.version;
                    finalSchemas.add(recordSchema);
                });

            }
            schemas = finalSchemas;
        }

        // compose indexes response
        if (null != indexesToFetch && indexesToFetch.isValid()) {
            final List<RecordIndex> finalIndexes = new ArrayList<>();
            if (indexesToFetch.isFetchAll) {
                finalIndexes.add(allocRecordIndex.apply(null));
            } else {
                indexesToFetch.metaNames.forEach(
                        name -> finalIndexes.add(allocRecordIndex.apply(name)));
            }
            indexes = finalIndexes;
        }

        // compose tags response
        if (null != tagsToFetch && tagsToFetch.isValid()) {
            final List<RecordTag> finalTags = new ArrayList<>();
            if (tagsToFetch.isFetchAll) {
                finalTags.add(allocRecordTag.apply(null));
            } else {
                tagsToFetch.metaNames.forEach(
                        name -> finalTags.add(allocRecordTag.apply(name)));
            }
        }

        short versionId = version();
        switch (versionId) {
            case 0:
                return new FetchRecordMetasResponse(topic, schemas, indexes, tags, versionId);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.FETCH_RECORD_METAS.latestVersion()));
        }
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return getErrorResponse(e);
    }

    public String topic() {
        return this.topic;
    }

    public RecordMetasToFetch schemas() {
        return this.schemasToFetch;
    }

    public RecordMetasToFetch indexes() {
        return this.indexesToFetch;
    }

    public RecordMetasToFetch tags() {
        return this.tagsToFetch;
    }

    public static FetchRecordMetasRequest parse(ByteBuffer buffer, int versionId) {
        return new FetchRecordMetasRequest(ApiKeys.FETCH_RECORD_METAS.parseRequest( (short)versionId, buffer),
                (short) versionId);
    }

    public static FetchRecordMetasRequest parse(ByteBuffer buffer) {
        return parse(buffer, ApiKeys.FETCH_RECORD_METAS.latestVersion());
    }
}
