package com.aliyun.dts.subscribe.clients;

import com.aliyun.dts.subscribe.clients.common.Checkpoint;
import com.aliyun.dts.subscribe.clients.common.Util;
import com.aliyun.dts.subscribe.clients.metastore.MetaStore;
import com.aliyun.dts.subscribe.clients.metrics.DTSMetrics;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aliyun.dts.subscribe.clients.recordfetcher.Names.*;

public class ConsumerContext {
    private Properties properties;

    private String brokerUrl;
    private String topic;
    private String sid;
    private String user;
    private String password;

    private String initialCheckpoint;

    private boolean isForceUseCheckpoint;

    private ConsumerContext.ConsumerSubscribeMode subscribeMode;

    private Collection<TopicPartition> topicPartitions;

    private MetaStore<Checkpoint> userRegisteredStore;

    private long checkpointCommitInterval = 5000;

    private DTSMetrics dtsMetrics;

    private AtomicBoolean exited = new AtomicBoolean(false);

    public ConsumerContext(String brokerUrl, String topic, String sid, String userName, String password,
                           String initialCheckpoint, ConsumerContext.ConsumerSubscribeMode subscribeMode) {
        properties = new Properties();
        this.brokerUrl = brokerUrl;
        this.topic = topic;
        this.sid = sid;
        this.user = userName;
        this.password = password;
        this.initialCheckpoint = initialCheckpoint;
        this.subscribeMode = subscribeMode;
        this.dtsMetrics = new DTSMetrics();
    }

    public boolean isExited() {
        return this.exited.get();
    }

    public synchronized void exit() {
        dtsMetrics.close();
        this.exited.set(true);
    }

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Checkpoint getInitialCheckpoint() {
        return Util.parseCheckpoint(initialCheckpoint);
    }

    public void setInitialCheckpoint(String initialCheckpoint) {
        this.initialCheckpoint = initialCheckpoint;
    }

    public boolean isForceUseCheckpoint() {
        return isForceUseCheckpoint;
    }

    public void setForceUseCheckpoint(boolean isForceUseCheckpoint) {
        this.isForceUseCheckpoint = isForceUseCheckpoint;
    }

    public ConsumerSubscribeMode getSubscribeMode() {
        return this.subscribeMode;
    }

    public void setSubscribeMode(ConsumerContext.ConsumerSubscribeMode subscribeMode) {
        this.subscribeMode = subscribeMode;
    }

    public Collection<TopicPartition> getTopicPartitions() {
        return topicPartitions;
    }

    public void setTopicPartitions(Collection<TopicPartition> topicPartitions) {
        this.topicPartitions = topicPartitions;
    }

    public boolean hasValidTopicPartitions() {
        return topicPartitions != null && topicPartitions.size() > 0;
    }

    public MetaStore<Checkpoint> getUserRegisteredStore() {
        return userRegisteredStore;
    }

    public void setUserRegisteredStore(MetaStore<Checkpoint> userRegisteredStore) {
        this.userRegisteredStore = userRegisteredStore;
    }

    public Properties getKafkaProperties() {
        properties.setProperty(USER_NAME, this.user);
        properties.setProperty(PASSWORD_NAME, this.password);
        properties.setProperty(SID_NAME, this.sid);
        properties.setProperty(GROUP_NAME, this.sid);
        properties.setProperty(KAFKA_TOPIC, this.topic);
        properties.setProperty(KAFKA_BROKER_URL_NAME, this.brokerUrl);

        return properties;
    }

    public void setProperty(String key, String value) {
        this.properties.setProperty(key, value);
    }

    public String getGroupID() {
        return this.sid;
    }

    public long getCheckpointCommitInterval() {
        return checkpointCommitInterval;
    }

    public void setCheckpointCommitInterval(long checkpointCommitInterval) {
        this.checkpointCommitInterval = checkpointCommitInterval;
    }

    public DTSMetrics getDtsMetrics() {
        return dtsMetrics;
    }

    public enum ConsumerSubscribeMode {
        ASSIGN,
        SUBSCRIBE,
        UNKNOWN;
    }
}
