package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.meta.RecordIndex;
import org.apache.kafka.common.record.meta.RecordMeta;
import org.apache.kafka.common.record.meta.RecordSchema;
import org.apache.kafka.common.record.meta.RecordTag;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.kafka.common.protocol.types.Type.*;

public class CreateRecordMetasResponse extends AbstractResponse {

    private static final String TOPIC_KEY_NAME = "topic";

    private static final String RECORD_SCHEMA_ERRORS_KEY_NAME = "schemas_errors";
    private static final String RECORD_INDEX_ERRORS_KEY_NAME = "indexes_errors";
    private static final String RECORD_TAG_ERRORS_KEY_NAME = "tags_errors";

    private static final String ERROR_META_KEY_NAME = "name";
    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String ERROR_MESSAGE_KEY_NAME = "error_message";

    private static final String SCHEMA_ERROR_META_KEY_NAME = "meta_response";
    private static final String SCHEMA_ID_KEY_NAME = "id";
    private static final String SCHEMA_VERSION_KEY_NAME = "version";

    public static final Schema CREATE_META_RESPONSE_V0 = new Schema(
            new Field("name", STRING, "The name of meta"),
            new Field("error_code", INT16, "The error code for this meta"),
            new Field("error_message", NULLABLE_STRING, "The error message for this meta")
    );

    public static final Schema CREATE_SCHEMA_RESPONSE_V0 = new Schema(
            new Field("meta_response", CREATE_META_RESPONSE_V0, "The meta response for schema"),
            new Field("id", INT32, "The ID of record schema created"),
            new Field("version", INT16, "The version of record schema created")
    );

    public static final Schema CREATE_RECORD_METAS_RESPONSE_V0 = new Schema(
            new Field("topic", STRING, "The name of topic"),
            new Field("schemas_errors", ArrayOf.nullable(CREATE_SCHEMA_RESPONSE_V0)),
            new Field("indexes_errors", ArrayOf.nullable(CREATE_META_RESPONSE_V0)),
            new Field("tags_errors", ArrayOf.nullable(CREATE_META_RESPONSE_V0))
    );

    public static Schema[] schemaVersions() {
        return new Schema[]{CREATE_RECORD_METAS_RESPONSE_V0};
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return null;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.CREATE_RECORD_METAS.responseSchema(version));

        struct.set(TOPIC_KEY_NAME, topic);
        setErrorStructs(struct, RECORD_SCHEMA_ERRORS_KEY_NAME, schemaErrors);
        setErrorStructs(struct, RECORD_INDEX_ERRORS_KEY_NAME, indexErrors);
        setErrorStructs(struct, RECORD_TAG_ERRORS_KEY_NAME, tagErrors);
        return struct;
    }

    public static class Error {
        private final Errors error;
        private final String message; // introduced in V1

        public Error(Errors error) {
            this(error, null);
        }

        public Error(Errors error, String message) {
            this.error = error;
            this.message = message;
        }

        public boolean is(Errors error) {
            return this.error == error;
        }

        public Errors error() {
            return error;
        }

        public String message() {
            return message;
        }

        public String messageWithFallback() {
            if (message == null)
                return error.message();
            return message;
        }

        @Override
        public String toString() {
            return "Error(error=" + error + ", message=" + message + ")";
        }
    }

    private final String topic;
    private final List<RecordSchema> schemaErrors;
    private final List<RecordIndex> indexErrors;
    private final List<RecordTag> tagErrors;

    private <T extends RecordMeta> void setErrorStructs(Struct struct, String keyName, List<T> metas) {
        if (null != metas) {
            List<Struct> errorStructs = new ArrayList<>(metas.size());
            metas.forEach(meta -> {
                Struct errorStruct = struct.instance(keyName);
                Struct origErrorStruct = errorStruct;
                if (meta instanceof RecordSchema) {
                    RecordSchema recordSchema = (RecordSchema) meta;
                    errorStruct.set(SCHEMA_ID_KEY_NAME, recordSchema.getId());
                    errorStruct.set(SCHEMA_VERSION_KEY_NAME, recordSchema.getVersion());
                    Struct subStruct = errorStruct.instance(SCHEMA_ERROR_META_KEY_NAME);
                    errorStruct.set(SCHEMA_ERROR_META_KEY_NAME, subStruct);
                    errorStruct = subStruct;
                }
                errorStruct.set(ERROR_META_KEY_NAME, meta.getName());
                errorStruct.set(ERROR_CODE_KEY_NAME, meta.getError().code());
                errorStructs.add(origErrorStruct);
            });

            struct.set(keyName, errorStructs.toArray());
        }
    }

    private <T extends RecordMeta> List<T> extractFromErrorStructs(String keyName, Struct struct, Function<Struct, T> alloc) {
        List<T> ret = null;
        Object[] errorObjects = struct.getArray(keyName);
        if (null != errorObjects) {
            ret = new ArrayList<>();
            for (Object object : errorObjects) {
                Struct errorStruct = (Struct) object;
                T item = alloc.apply(errorStruct);
                ret.add(item);
            }
        }
        return ret;
    }

    public CreateRecordMetasResponse(String topic, List<RecordSchema> schemaErrors, List<RecordIndex> indexErrors,
                                     List<RecordTag> tagErrors) {
        this(topic, schemaErrors, indexErrors, tagErrors, ApiKeys.CREATE_RECORD_METAS.latestVersion());
    }

    public CreateRecordMetasResponse(String topic, List<RecordSchema> schemaErrors, List<RecordIndex> indexErrors,
                                     List<RecordTag> tagErrors, short version) {


        this.topic = topic;
        this.schemaErrors = schemaErrors;
        this.indexErrors = indexErrors;
        this.tagErrors = tagErrors;
    }

    //BUG: the tag error and index error parsed  is not insist with the response from server
    public CreateRecordMetasResponse(Struct struct) {

        this.topic = struct.getString(TOPIC_KEY_NAME);
        this.schemaErrors = extractFromErrorStructs(RECORD_SCHEMA_ERRORS_KEY_NAME, struct, errorStruct -> {
            Struct subStruct = errorStruct.getStruct(SCHEMA_ERROR_META_KEY_NAME);
            return new RecordSchema(subStruct.getString(ERROR_META_KEY_NAME),
                    errorStruct.getInt(SCHEMA_ID_KEY_NAME), errorStruct.getShort(SCHEMA_VERSION_KEY_NAME),
                    Errors.forCode(subStruct.getShort(ERROR_CODE_KEY_NAME)));
        });
        this.indexErrors = extractFromErrorStructs(RECORD_INDEX_ERRORS_KEY_NAME, struct, errorStruct ->
                new RecordIndex(errorStruct.getString(ERROR_META_KEY_NAME), Errors.forCode(errorStruct.getShort(ERROR_CODE_KEY_NAME)))
        );
        this.tagErrors = extractFromErrorStructs(RECORD_TAG_ERRORS_KEY_NAME, struct, errorStruct -> {
            return new RecordTag(errorStruct.getString(ERROR_META_KEY_NAME), Errors.forCode(errorStruct.getShort(ERROR_CODE_KEY_NAME)));
        });
    }

    public String topic() {
        return topic;
    }

    public List<RecordSchema> schemaErrors() {
        return schemaErrors;
    }

    public List<RecordIndex> indexErrors() {
        return indexErrors;
    }

    public List<RecordTag> tagErrors() {
        return tagErrors;
    }

    public static CreateRecordMetasResponse parse(ByteBuffer buffer) {
        return new CreateRecordMetasResponse(ApiKeys.CREATE_RECORD_METAS.responseSchema(ApiKeys.CREATE_RECORD_METAS.latestVersion()).read(buffer));
    }

    public static CreateRecordMetasResponse parse(ByteBuffer buffer, int version) {
        return new CreateRecordMetasResponse(ApiKeys.CREATE_RECORD_METAS.responseSchema((short)version).read(buffer));
    }
    //TODO: add throttle constructor
    public int throttleTimeMs() {
        return 0;
    }
}
