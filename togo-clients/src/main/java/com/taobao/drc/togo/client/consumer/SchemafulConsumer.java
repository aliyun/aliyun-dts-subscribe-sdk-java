package com.taobao.drc.togo.client.consumer;

import com.taobao.drc.togo.common.businesslogic.TagMatchFunction;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * @see TogoConsumer
 */
public interface SchemafulConsumer extends Closeable {
    SchemafulConsumerRecords poll(long timeout);

    void setFetchRule(TopicPartition topicPartition, FetchRule fetchRule);

    void setFetchMatchFunction(TagMatchFunction tagMatchFunction);

    Set<TopicPartition> assignment();

    Set<String> subscription();

    void subscribe(Collection<String> topics);

    void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

    void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

    void assign(Collection<TopicPartition> partitions);

    void unsubscribe();

    void commitSync();

    void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);

    void commitAsync();

    void commitAsync(OffsetCommitCallback callback);

    void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

    void seek(TopicPartition partition, long offset);

    void seekToBeginning(Collection<TopicPartition> partitions);

    void seekToEnd(Collection<TopicPartition> partitions);

    long position(TopicPartition partition);

    OffsetAndMetadata committed(TopicPartition partition);

    Map<MetricName, ? extends Metric> metrics();

    List<PartitionInfo> partitionsFor(String topic);

    Map<String, List<PartitionInfo>> listTopics();

    Set<TopicPartition> paused();

    void pause(Collection<TopicPartition> partitions);

    void resume(Collection<TopicPartition> partitions);

    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch);

    Map<TopicPartition, OffsetAndQuery> offsetsForFirstMatch(Map<TopicPartition, QueryCondition> timestampsToConditions);

    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions);

    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions);

    void close();

    void close(long timeout, TimeUnit unit);

    void wakeup();
}
