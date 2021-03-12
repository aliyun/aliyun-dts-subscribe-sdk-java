package com.aliyun.dts.subscribe.clients.metastore;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.dts.subscribe.clients.common.Checkpoint;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public abstract class AbstractUserMetaStore implements MetaStore<Checkpoint> {
    private static final String GROUP_ID_NAME = "groupID";
    private static final String STREAM_CHECKPOINT_NAME = "streamCheckpoint";
    private static final String TOPIC_NAME = "topic";
    private static final String PARTITION_NAME = "partition";
    private static final String OFFSET_NAME = "offset";
    private static final String TIMESTAMP_NAME = "timestamp";
    private static final String INFO_NAME = "info";

    private static class StoreElement {
        final String groupName;
        final Map<TopicPartition, Checkpoint> streamCheckpoint;

        private StoreElement(String groupName, Map<TopicPartition, Checkpoint> streamCheckpoint) {
            this.groupName = groupName;
            this.streamCheckpoint = streamCheckpoint;
        }
    }

    private String toJson(StoreElement storeElement) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(GROUP_ID_NAME, storeElement.groupName);
        JSONArray jsonArray = new JSONArray();
        storeElement.streamCheckpoint.forEach((tp, checkpoint) -> {
            JSONObject streamCheckpointJsonObject = new JSONObject();
            streamCheckpointJsonObject.put(TOPIC_NAME, tp.topic());
            streamCheckpointJsonObject.put(PARTITION_NAME, tp.partition());
            streamCheckpointJsonObject.put(OFFSET_NAME, checkpoint.getOffset());
            streamCheckpointJsonObject.put(TIMESTAMP_NAME, checkpoint.getTimeStamp());
            streamCheckpointJsonObject.put(INFO_NAME, checkpoint.getInfo());
            jsonArray.add(streamCheckpointJsonObject);
        });

        jsonObject.put(STREAM_CHECKPOINT_NAME, jsonArray);
        return jsonObject.toJSONString();
    }

    private StoreElement fromString(String jsonString) {
        JSONObject jsonObject = JSONObject.parseObject(jsonString);
        String groupName = jsonObject.getString(GROUP_ID_NAME);
        JSONArray streamCheckpointJsonObject = jsonObject.getJSONArray(STREAM_CHECKPOINT_NAME);
        Map<TopicPartition, Checkpoint> checkpointInfo = new HashMap<>();
        for (Object o : streamCheckpointJsonObject) {
            JSONObject tpAndCheckpoint = (JSONObject) o;
            String topic = tpAndCheckpoint.getString(TOPIC_NAME);
            int partition = tpAndCheckpoint.getInteger(PARTITION_NAME);
            long offset = tpAndCheckpoint.getLong(OFFSET_NAME);
            long timestamp = tpAndCheckpoint.getLong(TIMESTAMP_NAME);
            String info = tpAndCheckpoint.getString(INFO_NAME);
            checkpointInfo.put(new TopicPartition(topic, partition), new Checkpoint(new TopicPartition(topic, partition), timestamp, offset, info));
        }

        return new StoreElement(groupName, checkpointInfo);
    }

    @Override
    public Future<Checkpoint> serializeTo(TopicPartition topicPartition, String groupID, Checkpoint value) {

        Map<TopicPartition, Checkpoint> topicPartitionCheckpoint = new HashMap<>();
        topicPartitionCheckpoint.put(topicPartition, value);

        String toStoreJson = toJson(new StoreElement(groupID, topicPartitionCheckpoint));

        saveData(groupID, toStoreJson);

        KafkaFutureImpl ret =  new KafkaFutureImpl<>();
        ret.complete(value);
        return ret;
    }

    protected abstract void saveData(String groupID, String toStoreJson);

    @Override
    public Checkpoint deserializeFrom(TopicPartition topicPartition, String groupID) {

        String checkpointData = getData(groupID);
        StoreElement storeElement = fromString(checkpointData);
        if (StringUtils.equals(storeElement.groupName, groupID)) {
            return storeElement.streamCheckpoint.get(topicPartition);
        }

        return null;
    }

    protected abstract String getData(String groupID);
}
