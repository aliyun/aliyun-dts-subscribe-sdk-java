package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.types.Type.*;

public class FlexListOffsetResponse extends AbstractResponse {
    public static final long UNKNOWN_OFFSET = -1L;
    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partition_responses";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String ERROR_CODE_KEY_NAME = "error_code";

    private static final String OFFSET_KEY_NAME = "offset";

    private final Map<TopicPartition, PartitionData> responseData;
    @Override
    public Map<Errors, Integer> errorCounts() {
        return null;
    }

    public static final Schema FLEX_LIST_OFFSET_RESPONSE_PARTITION_V0 = new Schema(
            new Field("partition", INT32, "Topic partition id."),
            new Field("error_code", INT16),
            new Field("offset", INT64, "offset found")
    );

    public static final Schema FLEX_LIST_OFFSET_RESPONSE_TOPIC_V0 = new Schema(
            new Field("topic", STRING),
            new Field("partition_responses", new ArrayOf(FLEX_LIST_OFFSET_RESPONSE_PARTITION_V0))
    );

    public static final Schema FLEX_LIST_OFFSET_RESPONSE_V0 = new Schema(
            new Field("responses", new ArrayOf(FLEX_LIST_OFFSET_RESPONSE_TOPIC_V0))
    );

    public static Schema[] schemaVersions() {
        return new Schema[]{FLEX_LIST_OFFSET_RESPONSE_V0};
    }

    @Override
    protected Struct toStruct(short version) {
        Map<String, Map<Integer, PartitionData>> topicsData = CollectionUtils.groupDataByTopic(responseData);
        Struct struct = new Struct(ApiKeys.FLEX_LIST_OFFSET.responseSchema((short)version));
        List<Struct> topicArray = new ArrayList<Struct>();
        for (Map.Entry<String, Map<Integer, PartitionData>> topicEntry: topicsData.entrySet()) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.getKey());
            List<Struct> partitionArray = new ArrayList<Struct>();
            for (Map.Entry<Integer, PartitionData> partitionEntry : topicEntry.getValue().entrySet()) {
                PartitionData offsetPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                partitionData.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                partitionData.set(ERROR_CODE_KEY_NAME, offsetPartitionData.errorCode);
                partitionData.set(OFFSET_KEY_NAME, offsetPartitionData.offset);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicArray.toArray());
        return struct;
    }

    public static final class PartitionData {
        public final short errorCode;
        public final int partition;
        public final Long offset;

        public PartitionData(short errorCode, int partition, long offset) {
            this.errorCode = errorCode;
            this.partition = partition;
            this.offset = offset;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("PartitionData{").
                    append("errorCode: ").append((int) errorCode).
                    append(", partition: ").append(partition).
                    append(", offset: ").append(offset).
                    append("}");
            return bld.toString();
        }
    }

    public FlexListOffsetResponse(Map<TopicPartition, PartitionData> responseData) {
        this.responseData = responseData;
    }

    public FlexListOffsetResponse(Struct struct) {
        responseData = new HashMap<>();
        for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                short errorCode = partitionResponse.getShort(ERROR_CODE_KEY_NAME);

                long offset = partitionResponse.getLong(OFFSET_KEY_NAME);
                PartitionData partitionData = new PartitionData(errorCode, partition, offset);
                responseData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
    }

    public Map<TopicPartition, PartitionData> responseData() {
        return responseData;
    }

    public static FlexListOffsetResponse parse(ByteBuffer buffer) {
        return new FlexListOffsetResponse(ApiKeys.FLEX_LIST_OFFSET.responseSchema(ApiKeys.FLEX_LIST_OFFSET.latestVersion()).read(buffer));
    }

    public static FlexListOffsetResponse parse(ByteBuffer buffer, int version) {
        return new FlexListOffsetResponse(ApiKeys.FLEX_LIST_OFFSET.responseSchema((short)version).read(buffer));
    }
    //TODO: add throttle constructor
    public int throttleTimeMs() {
        return 0;
    }
}
