package com.aliyun.dts.subscribe.clients.metastore;

import com.aliyun.dts.subscribe.clients.common.Checkpoint;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MetaStoreCenter {
    private static final Logger log = LoggerFactory.getLogger(MetaStoreCenter.class);
    private static final Logger core_log = LoggerFactory.getLogger("log.metrics");

    private final Map<String, MetaStore<Checkpoint>> registeredStore = new HashMap<>();
    public MetaStoreCenter() {

    }

    public void registerStore(String name, MetaStore metaStore) {
        log.info("MetaStoreCenter: register metaStore {}", name);
        registeredStore.put(name, metaStore);
    }

    public void store(TopicPartition topicPartition, String group, Checkpoint value) {
        registeredStore.values().forEach(v -> {
            v.serializeTo(topicPartition, group, value);
        });
    }

    public Checkpoint seek(String storeName, TopicPartition tp, String group) {
        MetaStore<Checkpoint> metaStore = registeredStore.get(storeName);
        if (null != metaStore) {
            Checkpoint checkpoint = metaStore.deserializeFrom(tp, group);
            core_log.info("MetaStoreCenter: seek storeName {}, topicPartition {}, group {}, checkpoint {}", storeName, tp, group, checkpoint);
            return checkpoint;
        } else {
            core_log.info("MetaStoreCenter: seek storeName {}, topicPartition {}, group {}, checkpoint {}", storeName, tp, group, null);
            return null;
        }
    }
}
