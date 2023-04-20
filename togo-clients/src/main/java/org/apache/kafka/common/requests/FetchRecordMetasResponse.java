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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.types.Type.INT16;
import static org.apache.kafka.common.protocol.types.Type.STRING;
import static org.apache.kafka.common.requests.CreateRecordMetasRequest.*;


public class FetchRecordMetasResponse extends AbstractResponse {

    private static final String TOPIC_KEY_NAME = "topic";
    private static final String SCHEMAS_KEY_NAME = "schemas";
    private static final String INDEXES_KEY_NAME  = "indexes";
    private static final String TAGS_KEY_NAME = "tags";

    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String SCHEMA_INFO_KEY_NAME = "schema";
    private static final String INDEX_INFO_KEY_NAME = "index";
    private static final String TAG_INFO_KEY_NAME = "tag";

    private final String topic;
    private final List<RecordSchema> schemas;
    private final List<RecordIndex> indexes;
    private final List<RecordTag> tags;

    public static final Schema RECORD_SCHEMA_RESPONSE_V0 = new Schema(
            new Field("error_code", INT16, "Error code for this schema"),
            new Field("schema", RECORD_SCHEMA_V0, "Single schema for fetching meta response")
    );

    public static final Schema RECORD_INDEX_RESPONSE_V0 = new Schema(
            new Field("error_code", INT16, "Error code for this index"),
            new Field("index", RECORD_INDEX_V0, "Single index for fetching meta response")
    );

    public static final Schema RECORD_TAG_RESPONSE_V0 = new Schema(
            new Field("error_code", INT16, "Error code for this tag"),
            new Field("tag", RECORD_TAG_V0, "Single tag for fetching meta response")
    );

    public static final Schema FETCH_RECORD_META_RESPONSE_V0 = new Schema(
            new Field("topic", STRING, "The name for topic"),
            new Field("schemas", ArrayOf.nullable(RECORD_SCHEMA_RESPONSE_V0), "The fetched schemas"),
            new Field("indexes", ArrayOf.nullable(RECORD_INDEX_RESPONSE_V0), "The fetched indexes"),
            new Field("tags", ArrayOf.nullable(RECORD_TAG_RESPONSE_V0), "The fetched tags")
    );

    public static Schema[] schemaVersions() {
        return new Schema[]{FETCH_RECORD_META_RESPONSE_V0};
    }

    public FetchRecordMetasResponse(String topic,
                                    List<RecordSchema> schemas,
                                    List<RecordIndex> indexes,
                                    List<RecordTag> tags) {
        this(topic, schemas, indexes, tags, ApiKeys.FETCH_RECORD_METAS.latestVersion());
    }

    /**
     * Constructor for a specific version
     */
    public FetchRecordMetasResponse(String topic,
                                    List<RecordSchema> schemas,
                                    List<RecordIndex> indexes,
                                    List<RecordTag> tags,
                                    int version) {


        this.topic = topic;
        this.schemas = schemas;
        this.indexes = indexes;
        this.tags = tags;
    }

    public FetchRecordMetasResponse(Struct struct) {

        this.topic = struct.getString(TOPIC_KEY_NAME);

        // extract schemas
        Object[] schemaObjects = struct.getArray(SCHEMAS_KEY_NAME);
        if (null != schemaObjects) {
            schemas = new ArrayList<>();
            for (Object schemaObject : schemaObjects) {
                Struct schemaStruct = (Struct) schemaObject;
                RecordSchema recordSchema = RecordSchema.parseFrom(schemaStruct.getStruct(SCHEMA_INFO_KEY_NAME));
                recordSchema.setError(Errors.forCode(schemaStruct.getShort(ERROR_CODE_KEY_NAME)));
                schemas.add(recordSchema);
            }
        } else {
            schemas = null;
        }

        // extract indexes
        Object[] indexObjects = struct.getArray(INDEXES_KEY_NAME);
        if (null != indexObjects) {
            indexes = new ArrayList<>();
            for (Object indexObject : indexObjects) {
                Struct indexStruct = (Struct) indexObject;
                RecordIndex recordIndex = RecordIndex.parseFrom(indexStruct.getStruct(INDEX_INFO_KEY_NAME));
                recordIndex.setError(Errors.forCode(indexStruct.getShort(ERROR_CODE_KEY_NAME)));
                indexes.add(recordIndex);
            }
        } else {
            indexes = null;
        }

        // extract tags
        Object[] tagObjects = struct.getArray(TAGS_KEY_NAME);
        if (null != tagObjects) {
            tags = new ArrayList<>();
            for (Object tagObject : tagObjects) {
                Struct tagStruct = (Struct) tagObject;
                RecordTag recordTag = RecordTag.parseFrom((tagStruct.getStruct(TAG_INFO_KEY_NAME)));
                recordTag.setError(Errors.forCode(tagStruct.getShort(ERROR_CODE_KEY_NAME)));
                tags.add(recordTag);
            }
        } else {
            tags = null;
        }
    }

    public static FetchRecordMetasResponse parse(ByteBuffer buffer) {
        return parse(buffer, ApiKeys.FETCH_RECORD_METAS.latestVersion());
    }

    public static FetchRecordMetasResponse parse(ByteBuffer buffer, int version) {
        return new FetchRecordMetasResponse(ApiKeys.FETCH_RECORD_METAS.responseSchema((short)version).read(buffer));
    }

    public String getTopic() {
        return topic;
    }

    public List<RecordSchema> schemas() {
        return schemas;
    }

    public List<RecordIndex> indexes() {
        return indexes;
    }

    public List<RecordTag> tags() {
        return tags;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return null;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.FETCH_RECORD_METAS.responseSchema(version));

        struct.set(TOPIC_KEY_NAME, topic);

        // schemas response
        if (null != schemas) {
            List<Struct> schemaStructs = new ArrayList<>(schemas.size());
            schemas.forEach(schema -> {
                Struct schemaStruct = struct.instance(SCHEMAS_KEY_NAME);
                schemaStruct.set(ERROR_CODE_KEY_NAME, schema.getError().code());
                schemaStruct.set(SCHEMA_INFO_KEY_NAME, schema.toStruct(SCHEMA_INFO_KEY_NAME, schemaStruct));
                schemaStructs.add(schemaStruct);
            });
            struct.set(SCHEMAS_KEY_NAME, schemaStructs.toArray());
        }

        // indexes response
        if (null != indexes) {
            List<Struct> indexStructs = new ArrayList<>(indexes.size());
            indexes.forEach(index -> {
                Struct indexStruct = struct.instance(INDEXES_KEY_NAME);
                indexStruct.set(ERROR_CODE_KEY_NAME, index.getError().code());
                indexStruct.set(INDEX_INFO_KEY_NAME, index.toStruct(INDEX_INFO_KEY_NAME, indexStruct));
                indexStructs.add(indexStruct);
            });
            struct.set(INDEXES_KEY_NAME, indexStructs.toArray());
        }

        // tags repsonse
        if (null != tags) {
            List<Struct> tagStructs = new ArrayList<>(tags.size());
            tags.forEach(tag -> {
                Struct tagStruct = struct.instance(TAGS_KEY_NAME);
                tagStruct.set(ERROR_CODE_KEY_NAME, tag.getError().code());
                tagStruct.set(TAG_INFO_KEY_NAME, tag.toStruct(TAG_INFO_KEY_NAME, tagStruct));
                tagStructs.add(tagStruct);
            });
            struct.set(TAGS_KEY_NAME, tagStructs.toArray());
        }
        return struct;
    }
    //TODO: add throttle constructor
    public int throttleTimeMs() {
        return 0;
    }
}
