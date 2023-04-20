package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.types.Type.*;

public class SetFetchTagsResponse extends AbstractResponse {

    private static final String RESPONSES_KEY_NAME = "responses";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partition_responses";
    private static final String THROTTLE_TIME_KEY_NAME = "throttle_time_ms";

    // partition level field names
    private static final String PARTITION_HEADER_KEY_NAME = "partition_header";
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String ERROR_CODE_KEY_NAME = "error_code";


    private final LinkedHashMap<TopicPartition, PartitionResponse> responseData;

    @Override
    public Map<Errors, Integer> errorCounts() {
        return null;
    }

    public static final Schema SET_FETCH_TAGS_RESPONSE_PARTITION_HEADER_V0 = new Schema(
            new Field("partition", INT32, "Topic partition id."),
            new Field("error_code", INT16)
    );

    public static final Schema SET_FETCH_TAGS_RESPONSE_PARTITION_V0 = new Schema(
            new Field("partition_header", SET_FETCH_TAGS_RESPONSE_PARTITION_HEADER_V0)
    );

    public static final Schema SET_FETCH_TAGS_RESPONSE_TOPIC_V0 = new Schema(
            new Field("topic", STRING),
            new Field("partition_responses", new ArrayOf(SET_FETCH_TAGS_RESPONSE_PARTITION_V0))
    );

    public static final Schema SET_FETCH_TAGS_RESPONSE_V0 = new Schema(
            new Field("responses", new ArrayOf(SET_FETCH_TAGS_RESPONSE_TOPIC_V0))
    );

    public static final Schema SET_FETCH_TAGS_RESPONSE_V1 =  SET_FETCH_TAGS_RESPONSE_V0;

    public static final Schema SET_FETCH_TAGS_RESPONSE_V2 =  SET_FETCH_TAGS_RESPONSE_V1;


    public static Schema[] schemaVersions() {
        return new Schema[]{SET_FETCH_TAGS_RESPONSE_V0, SET_FETCH_TAGS_RESPONSE_V1, SET_FETCH_TAGS_RESPONSE_V2};
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.SET_FETCH_TAGS.responseSchema(version));

        List<FetchRequest.TopicAndPartitionData<PartitionResponse>> topicsData = FetchRequest.TopicAndPartitionData.batchByTopic(responseData);
        List<Struct> topicArray = new ArrayList<>();
        for (FetchRequest.TopicAndPartitionData<PartitionResponse> topicEntry: topicsData) {
            Struct topicData = struct.instance(RESPONSES_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.topic);
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, PartitionResponse> partitionEntry : topicEntry.partitions.entrySet()) {
                PartitionResponse fetchPartitionData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITIONS_KEY_NAME);
                Struct partitionDataHeader = partitionData.instance(PARTITION_HEADER_KEY_NAME);
                partitionDataHeader.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                partitionDataHeader.set(ERROR_CODE_KEY_NAME, fetchPartitionData.errorCode);
                partitionData.set(PARTITION_HEADER_KEY_NAME, partitionDataHeader);
                partitionArray.add(partitionData);
            }
            topicData.set(PARTITIONS_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(RESPONSES_KEY_NAME, topicArray.toArray());
        return struct;
    }

    public static final class PartitionResponse {
        public final short errorCode;
        public final String errorMessage;

        public PartitionResponse(short errorCode) {
            this(errorCode, null);
        }

        public PartitionResponse(short errorCode, String errorMessage) {
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
        }

        @Override
        public String toString() {
            return "(errorCode=" + errorCode + ", errorMessage=" + errorMessage + ")";
        }
    }

    public SetFetchTagsResponse(LinkedHashMap<TopicPartition, PartitionResponse> responseData) {
        this(responseData, ApiKeys.SET_FETCH_TAGS.latestVersion());
    }

    /**
     * Constructor for all versions.
     *
     * @param responseData fetched data grouped by topic-partition
     */
    public SetFetchTagsResponse(LinkedHashMap<TopicPartition, PartitionResponse> responseData, int version) {
        this.responseData = responseData;
    }

    public SetFetchTagsResponse(Struct struct) {
        LinkedHashMap<TopicPartition, PartitionResponse> responseData = new LinkedHashMap<>();
        for (Object topicResponseObj : struct.getArray(RESPONSES_KEY_NAME)) {
            Struct topicResponse = (Struct) topicResponseObj;
            String topic = topicResponse.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicResponse.getArray(PARTITIONS_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                Struct partitionResponseHeader = partitionResponse.getStruct(PARTITION_HEADER_KEY_NAME);
                int partition = partitionResponseHeader.getInt(PARTITION_KEY_NAME);
                short errorCode = partitionResponseHeader.getShort(ERROR_CODE_KEY_NAME);
                PartitionResponse partitionData = new PartitionResponse(errorCode);
                responseData.put(new TopicPartition(topic, partition), partitionData);
            }
        }
        this.responseData = responseData;
    }

    public LinkedHashMap<TopicPartition, PartitionResponse> responseData() {
        return responseData;
    }

    public static SetFetchTagsResponse parse(ByteBuffer buffer) {
        return new SetFetchTagsResponse(ApiKeys.SET_FETCH_TAGS.requestSchema(ApiKeys.SET_FETCH_TAGS.latestVersion()).read(buffer));
    }

    public static SetFetchTagsResponse parse(ByteBuffer buffer, int version) {
        return new SetFetchTagsResponse(ApiKeys.SET_FETCH_TAGS.responseSchema((short)version).read(buffer));
    }
    //TODO: add throttle constructor
    public int throttleTimeMs() {
        return 0;
    }
}
