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
import java.util.List;

import static org.apache.kafka.common.protocol.types.Type.*;

public class CreateRecordMetasRequest extends AbstractRequest {
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String SCHEMAS_KEY_NAME = "schemas";
    private static final String INDEXES_KEY_NAME = "indexes";
    private static final String TAGS_KEY_NAME = "tags";


    // api CreateRecordMetas
    public static final Schema RECORD_SCHEMA_V0 = new Schema(
            new Field("name", STRING, "The name of record schema"),
            new Field("id", INT32, "The ID of record schema, for CreateRecordMetas, this is 0"),
            new Field("version", INT16, "The version of record schema, for CreateRecordMetas, this is 0"),
            new Field("data", BYTES, "The schema data")
    );

    public static final Schema RECORD_INDEX_V0 = new Schema(
            new Field("name", NULLABLE_STRING, "The name of index"),
            new Field("record_schema", NULLABLE_STRING, "The record schema is used to create index"),
            new Field("record_fields", ArrayOf.nullable(STRING), "The fields of record schema is used")
    );

    public static final Schema RECORD_TAG_V0 = new Schema(
            new Field("name", STRING, "The name of tag"),
            new Field("type", INT16, "The type of tag, filter tag or index tag"),
            new Field("record_schema", STRING, "The record schema is used to create tag"),
            new Field("fields_map_type", INT16, "The way to map record fields as tag"),
            new Field("record_fields", new ArrayOf(STRING), "The fields of record schema is used")
    );

    public static final Schema CREATE_RECORD_METAS_REQUEST_V0 = new Schema(
            new Field("topic", STRING, "The name of topic"),
            new Field("schemas", ArrayOf.nullable(RECORD_SCHEMA_V0), "The record schemas of this topic"),
            new Field("indexes", ArrayOf.nullable(RECORD_INDEX_V0), "The record indexes used for list offset"),
            new Field("tags", ArrayOf.nullable(RECORD_TAG_V0), "The records tags used for filtering records when subscribed")
    );

    public static Schema[] schemaVersions() {
        return new Schema[]{CREATE_RECORD_METAS_REQUEST_V0};
    }



    private final String topic;
    private final List<RecordSchema> schemas;
    private final List<RecordIndex> indexes;
    private final List<RecordTag> tags;

    private CreateRecordMetasRequest(String topic, List<RecordSchema> schemas, List<RecordIndex> indexes, List<RecordTag> tags, short version) {
        super(version);
        this.topic = topic;
        this.schemas = schemas;
        this.indexes = indexes;
        this.tags = tags;
    }

    public CreateRecordMetasRequest(Struct struct, short versionId) {
        super(versionId);

        this.topic = struct.getString(TOPIC_KEY_NAME);

        // extract schemas
        Object[] schemaStructs = struct.getArray(SCHEMAS_KEY_NAME);
        if (null != schemaStructs) {
            this.schemas = new ArrayList<>(schemaStructs.length);
            for (Object schemaObject : schemaStructs) {
                Struct schemaStruct = (Struct) schemaObject;
                RecordSchema recordSchema = RecordSchema.parseFrom(schemaStruct);
                this.schemas.add(recordSchema);
            }
        } else {
            this.schemas = null;
        }

        // extract indexes
        Object[] indexStructs = struct.getArray(INDEXES_KEY_NAME);
        if (null != indexStructs) {
            this.indexes = new ArrayList<>(indexStructs.length);
            for (Object indexObject : indexStructs) {
                Struct indexStruct = (Struct) indexObject;
                RecordIndex recordIndex = RecordIndex.parseFrom(indexStruct);
                this.indexes.add(recordIndex);
            }
        } else {
            this.indexes = null;
        }

        // extract tags
        Object[] tagStructs = struct.getArray(TAGS_KEY_NAME);
        if (null != tagStructs) {
            this.tags = new ArrayList<>(tagStructs.length);
            for (Object tagObject : tagStructs) {
                Struct tagStruct = (Struct) tagObject;
                RecordTag recordTag = RecordTag.parseFrom(tagStruct);
                this.tags.add(recordTag);
            }
        } else {
            this.tags = null;
        }
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.CREATE_RECORD_METAS.requestSchema(version()));

        struct.set(TOPIC_KEY_NAME, topic);

        // assign schemas
        if (null != schemas) {
            List<Struct> schemaStructs = new ArrayList<>(schemas.size());
            schemas.forEach(schema -> {
                Struct schemaStruct = schema.toStruct(SCHEMAS_KEY_NAME, struct);
                schemaStructs.add(schemaStruct);
            });
            struct.set(SCHEMAS_KEY_NAME, schemaStructs.toArray());
        }

        // assign indexes
        if (null != indexes) {
            List<Struct> indexStructs = new ArrayList<>(indexes.size());
            indexes.forEach(index -> {
                Struct indexStruct = index.toStruct(INDEXES_KEY_NAME, struct);
                indexStructs.add(indexStruct);
            });
            struct.set(INDEXES_KEY_NAME, indexStructs.toArray());
        }

        // assign tags
        if (null != tags) {
            List<Struct> tagStructs = new ArrayList<>(tags.size());
            tags.forEach(tag -> {
                Struct tagStruct = tag.toStruct(TAGS_KEY_NAME, struct);
                tagStructs.add(tagStruct);
            });
            struct.set(TAGS_KEY_NAME, tagStructs.toArray());
        }

        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        if (null != schemas) {
            schemas.forEach(schema -> schema.setError(Errors.forException(e)));
        }
        if (null != indexes) {
            indexes.forEach(index -> index.setError(Errors.forException(e)));
        }
        if (null != tags) {
            tags.forEach(tag -> tag.setError(Errors.forException(e)));
        }

        short versionId = version();
        switch (versionId) {
            case 0:
                return new CreateRecordMetasResponse(topic, schemas, indexes, tags, versionId);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.CREATE_RECORD_METAS.latestVersion()));
        }
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return getErrorResponse(e);
    }

    public String topic() {
        return this.topic;
    }

    public List<RecordSchema> schemas() {
        return this.schemas;
    }

    public List<RecordIndex> indexes() {
        return this.indexes;
    }

    public List<RecordTag> tags() {
        return this.tags;
    }

    public static CreateRecordMetasRequest parse(ByteBuffer buffer, int versionId) {
        return new CreateRecordMetasRequest(
                ApiKeys.CREATE_RECORD_METAS.parseRequest((short)versionId, buffer),
                (short) versionId);
    }

    public static CreateRecordMetasRequest parse(ByteBuffer buffer) {
        return parse(buffer, ApiKeys.CREATE_RECORD_METAS.latestVersion());
    }

    public static class Builder extends AbstractRequest.Builder<CreateRecordMetasRequest> {
        private final String topic;
        private final List<RecordSchema> schemas;
        private final List<RecordIndex> indexes;
        private final List<RecordTag> tags;

        public Builder(String topic, List<RecordSchema> schemas, List<RecordIndex> indexes, List<RecordTag> tags) {
            super(ApiKeys.CREATE_RECORD_METAS);
            this.topic = topic;
            this.schemas = schemas;
            this.indexes = indexes;
            this.tags = tags;
        }

        @Override
        public CreateRecordMetasRequest build(short version) {
            return new CreateRecordMetasRequest(topic, schemas, indexes, tags, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=CreateRecordMetas").
                    append(", topic=").append(topic).
                    append(", schemas=").append(Utils.mkString(schemas, ",")).
                    append(", indexes=").append(Utils.mkString(indexes, ",")).
                    append(", tags=").append(Utils.mkString(tags, ",")).
                    append(")");
            return bld.toString();
        }
    }
}
