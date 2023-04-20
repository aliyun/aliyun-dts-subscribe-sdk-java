package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.kafka.common.protocol.types.Type.*;


public class FlexListOffsetRequest extends AbstractRequest {


    private static final String REPLICA_ID_KEY_NAME = "replica_id";
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String TIMESTAMP_FROM_KEY_NAME = "timestamp_from";
    private static final String SCHEMA_NAME_KEY_NAME = "schema_name";
    private static final String INDEX_KEY_VALUES_KEY_NAME = "key_values";

    // index entry level field names
    private static final String INDEX_NAME_KEY_NAME = "field_name";
    private static final String INDEX_VALUE_KEY_NAME = "field_value";

    private final int replicaId;
    private final Map<TopicPartition, IndexEntry> partitionIndexEntries;
    private final Set<TopicPartition> duplicatePartitions;

    // api FlexListOffset
    public static final Schema FLEX_LIST_OFFSET_REQUEST_KEY_VALUE_V0 = new Schema(
            new Field("field_name", STRING, "The field name used to search"),
            new Field("field_value", STRING, "The value used to search")
    );

    public static final Schema FLEX_LIST_OFFSET_REQUEST_PARTITION_V0 = new Schema(
            new Field("partition", INT32, "Topic partition id"),
            new Field("timestamp_from", INT64, "The least timestamp to search, can be 0"),
            new Field("schema_name", NULLABLE_STRING, "The schema name used to search, null means the global index"),
            new Field("key_values", new ArrayOf(FLEX_LIST_OFFSET_REQUEST_KEY_VALUE_V0), "The key-values used to search")
    );

    public static final Schema FLEX_LIST_OFFSET_REQUEST_TOPIC_V0 = new Schema(
            new Field("topic", STRING, "Topic to list offset."),
            new Field("partitions", new ArrayOf(FLEX_LIST_OFFSET_REQUEST_PARTITION_V0), "Partitions to list offset.")
    );

    public static final Schema FLEX_LIST_OFFSET_REQUEST_V0 = new Schema(
            new Field("replica_id", INT32, "Broker id of the follower. For normal consumers, use -1."),
            new Field("topics", new ArrayOf(FLEX_LIST_OFFSET_REQUEST_TOPIC_V0), "Topics to list offsets.")
    );

    public static Schema[] schemaVersions() {
        return new Schema[]{FLEX_LIST_OFFSET_REQUEST_V0};
    }

    public static class FieldEntry {
        public final String fieldName;
        public final String fieldValue;

        public FieldEntry(String fieldName, String fieldValue) {
            this.fieldName = fieldName;
            this.fieldValue = fieldValue;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=FieldEntry").
                    append(", fieldName=").append(fieldName).
                    append(", fieldValue=").append(fieldValue).
                    append(")");
            return bld.toString();
        }
    }

    public static class IndexEntry {
        public final String schemaName;
        public final List<FieldEntry> fieldEntries;
        public final long timestampFrom;

        /*
         * This function is used to construct global search index entry
         */
        public IndexEntry(long timestampFrom, List<FieldEntry> fieldEntries) {
            this(timestampFrom, null, fieldEntries);
        }

        /*
         * Thie function is used to construct schema based search index entry
         */
        public IndexEntry(long timestampFrom, String schemaName, List<FieldEntry> fieldEntries) {
            this.schemaName = schemaName;
            this.fieldEntries = fieldEntries;
            this.timestampFrom = timestampFrom;
        }

        public boolean isGlobalIndexEntry() {
            return null == schemaName;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=IndexEntry").
                    append(", schemaName=").append(schemaName).
                    append(", fieldEntries=").append(Utils.mkString(fieldEntries, "|")).
                    append(", timestampFrom=").append(timestampFrom).
                    append(")");
            return bld.toString();
        }
    }

    public static class Builder extends AbstractRequest.Builder<FlexListOffsetRequest> {
        private final int replicaId;
        private Map<TopicPartition, IndexEntry> partitionIndexEntries = null;

        public Builder() {
            this(ListOffsetRequest.CONSUMER_REPLICA_ID);
        }

        public Builder(int replicaId) {
            super(ApiKeys.FLEX_LIST_OFFSET);
            this.replicaId = replicaId;
        }

        public Builder setTargetValues(Map<TopicPartition, IndexEntry> partitionIndexEntries) {
            this.partitionIndexEntries = partitionIndexEntries;
            return this;
        }

        @Override
        public FlexListOffsetRequest build(short version) {
            return new FlexListOffsetRequest(replicaId, partitionIndexEntries, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=FlexListOffsetRequest").
                    append(", replicaId=").append(replicaId).
                    append(", partitionIndexEntries=").append(Utils.mkString(partitionIndexEntries, "{", "}", "=", " ,")).
                    append(")");
            return bld.toString();
        }
    }

    /**
     * Private constructor with a specified version.
     */
    @SuppressWarnings("unchecked")
    private FlexListOffsetRequest(int replicaId, Map<TopicPartition, IndexEntry> targetIndexEntries, short version) {
        super( version);
        this.replicaId = replicaId;
        this.partitionIndexEntries = targetIndexEntries;
        this.duplicatePartitions = Collections.emptySet();
    }

    public FlexListOffsetRequest(Struct struct, short versionId) {
        super(versionId);
        Set<TopicPartition> duplicatePartitions = new HashSet<>();
        replicaId = struct.getInt(REPLICA_ID_KEY_NAME);

        partitionIndexEntries = new HashMap<>();
        for (Object topicObject : struct.getArray(TOPICS_KEY_NAME)) {
            Struct topicStruct = (Struct) topicObject;
            String topic = topicStruct.getString(TOPIC_KEY_NAME);
            for (Object partitionObject : topicStruct.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionStruct = (Struct) partitionObject;
                TopicPartition tp = new TopicPartition(topic,
                        partitionStruct.getInt(PARTITION_KEY_NAME));
                long timestampFrom = partitionStruct.getLong(TIMESTAMP_FROM_KEY_NAME);
                String schemaName = partitionStruct.getString(SCHEMA_NAME_KEY_NAME);
                Object[] keyValueObjects = partitionStruct.getArray(INDEX_KEY_VALUES_KEY_NAME);
                List<FieldEntry> fieldEntries = new ArrayList<>(keyValueObjects.length);
                for (Object keyValueObject : keyValueObjects) {
                    Struct keyValueStruct = (Struct) keyValueObject;
                    fieldEntries.add(new FieldEntry(
                            keyValueStruct.getString(INDEX_NAME_KEY_NAME),
                            keyValueStruct.getString(INDEX_VALUE_KEY_NAME)
                    ));
                }
                IndexEntry indexEntry = new IndexEntry(timestampFrom, schemaName, fieldEntries);
                if (null != partitionIndexEntries.put(tp, indexEntry)) {
                    duplicatePartitions.add(tp);
                }
            }
        }
        this.duplicatePartitions = duplicatePartitions;
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.FLEX_LIST_OFFSET.requestSchema(version()));
        struct.set(REPLICA_ID_KEY_NAME, replicaId);
        Map<String, Map<Integer, IndexEntry>> topicsData = CollectionUtils.groupDataByTopic(partitionIndexEntries);
        List<Struct> topicStructs = new ArrayList<>(topicsData.size());
        topicsData.forEach((topic, partitionDatas) -> {
            Struct topicStruct = struct.instance(TOPICS_KEY_NAME);
            topicStruct.set(TOPIC_KEY_NAME, topic);
            List<Struct> partitionsStruct = new ArrayList<>(partitionDatas.size());
            partitionDatas.forEach((partition, indexEntry) -> {
                Struct partitionStruct = topicStruct.instance(PARTITIONS_KEY_NAME);
                partitionStruct.set(PARTITION_KEY_NAME, partition);
                if (!indexEntry.isGlobalIndexEntry()) {
                    partitionStruct.set(SCHEMA_NAME_KEY_NAME, indexEntry.schemaName);
                }
                partitionStruct.set(TIMESTAMP_FROM_KEY_NAME, indexEntry.timestampFrom);
                List<Struct> keyValueStructs = new ArrayList<Struct>();
                indexEntry.fieldEntries.forEach(fieldEntry -> {
                    Struct keyValueStruct = partitionStruct.instance(INDEX_KEY_VALUES_KEY_NAME);
                    keyValueStruct.set(INDEX_NAME_KEY_NAME, fieldEntry.fieldName);
                    keyValueStruct.set(INDEX_VALUE_KEY_NAME, fieldEntry.fieldValue);
                    keyValueStructs.add(keyValueStruct);
                });
                partitionStruct.set(INDEX_KEY_VALUES_KEY_NAME, keyValueStructs.toArray());
                partitionsStruct.add(partitionStruct);
            });
            topicStruct.set(PARTITIONS_KEY_NAME, partitionsStruct.toArray());
            topicStructs.add(topicStruct);
        });
        struct.set(TOPICS_KEY_NAME, topicStructs.toArray());
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        Map<TopicPartition, FlexListOffsetResponse.PartitionData> responseData = new HashMap<>();

        short versionId = version();
        partitionIndexEntries.keySet().forEach(tp -> {
            FlexListOffsetResponse.PartitionData partitionResponse =
                    new FlexListOffsetResponse.PartitionData(Errors.forException(e).code(), tp.partition(), ListOffsetResponse.UNKNOWN_OFFSET);
            responseData.put(tp, partitionResponse);
        });

        switch (versionId) {
            case 0:
                return new FlexListOffsetResponse(responseData);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.LIST_OFFSETS.latestVersion()));
        }
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return getErrorResponse(e);
    }

    public int replicaId() {
        return replicaId;
    }

    public Map<TopicPartition, IndexEntry> partitionIndexEntries() {
        return partitionIndexEntries;
    }


    public static FlexListOffsetRequest parse(ByteBuffer buffer, int versionId) {
        return new FlexListOffsetRequest(ApiKeys.FLEX_LIST_OFFSET.parseRequest((short)versionId, buffer),
                (short) versionId);
    }

    public static FlexListOffsetRequest parse(ByteBuffer buffer) {
        return parse(buffer, ApiKeys.FLEX_LIST_OFFSET.latestVersion());
    }
}
