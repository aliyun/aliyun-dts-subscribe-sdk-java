package com.taobao.drc.togo.client.consumer;

import com.taobao.drc.togo.common.businesslogic.TagMatchFunction;
import com.taobao.drc.togo.data.Data;
import com.taobao.drc.togo.data.DataDeserializer;
import com.taobao.drc.togo.data.SchemaAndValue;
import com.taobao.drc.togo.data.SchemaNotFoundException;
import com.taobao.drc.togo.data.schema.SchemaDeserializer;
import com.taobao.drc.togo.data.schema.SchemaMetaData;
import com.taobao.drc.togo.data.schema.impl.DefaultSchemaMetaData;
import com.taobao.drc.togo.data.schema.impl.LocalSchemaCache;
import com.taobao.drc.togo.util.concurrent.TogoFuture;
import com.taobao.drc.togo.util.concurrent.TogoFutures;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.Fetcher;
import org.apache.kafka.clients.consumer.internals.FetcherMetricsRegistry;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.taobao.drc.togo.util.CommonName.TOGO_HEADER_KEY;

/**
 * @author yangyang
 * @since 17/3/29
 */
public class TogoAdaptedKafkaConsumer extends KafkaConsumer<byte[], byte[]> {
    private static final List<String> metaDataRequestProperties = Collections.singletonList("storage.engine.type=togo");

    private static final Logger logger = LoggerFactory.getLogger(TogoAdaptedKafkaConsumer.class);

    private final Map<SchemaRef, TogoFuture<TogoFetcher.RawSchemaMetaData>> pendingSchemas = new HashMap<>();
    private final Queue<FetchSchemaResult> completedSchemaFetches = new ConcurrentLinkedQueue<>();

    private final SchemaDeserializer schemaDeserializer = Data.get().schemaDeserializer();
    private final Map<String, Queue<PendingRecord>> pendingMessages = new HashMap<>();
    private final Queue<SchemafulConsumerRecord> completedRecords = new ArrayDeque<>();
    private final Map<String, LocalSchemaCache> schemaCacheMap = new HashMap<>();
    private final Map<String, DataDeserializer> deserializerMap = new HashMap<>();
    private TogoFetcher<byte[], byte[]> togoFetcher;

    private int maxPollRecords;

    public TogoAdaptedKafkaConsumer(Map<String, Object> properties) {
        super(properties);
    }

    String getClientId() {
        return clientId;
    }

    @Override
    protected NetworkClient createClient(Selectable selector, Metadata metadata, String clientId, int maxInFlightRequestsPerConnection, long reconnectBackoffMs,
                                         long reconnectBackoffMax, int socketSendBuffer, int socketReceiveBuffer, int requestTimeoutMs, Time time, boolean discoverBrokerVersions,
                                         ApiVersions apiVersions, Sensor throttleTimeSensor, LogContext logContext) {
        return new NetworkClient(selector,
                metadata,
                clientId,
                maxInFlightRequestsPerConnection,
                reconnectBackoffMs,
                reconnectBackoffMax,
                socketSendBuffer,
                socketReceiveBuffer,
                requestTimeoutMs,
                time,
                discoverBrokerVersions,
                apiVersions,
                throttleTimeSensor,
                logContext,
                metaDataRequestProperties);
    }

    @Override
    protected Fetcher<byte[], byte[]> createFetcher(LogContext logContext, ConsumerNetworkClient client,
                                                    int minBytes,
                                                    int maxBytes,
                                                    int maxWaitMs,
                                                    int fetchSize,
                                                    int maxPollRecords,
                                                    boolean checkCrcs,
                                                    Deserializer<byte[]> keyDeserializer,
                                                    Deserializer<byte[]> valueDeserializer,
                                                    Metadata metadata,
                                                    SubscriptionState subscriptions,
                                                    Metrics metrics,
                                                    FetcherMetricsRegistry metricGrpPrefix,
                                                    Time time,
                                                    long retryBackoffMs,
                                                    IsolationLevel isolationLevel) {
        togoFetcher = new TogoFetcher<>(logContext, client, minBytes, maxBytes, maxWaitMs, fetchSize, maxPollRecords, checkCrcs,
                keyDeserializer, valueDeserializer, metadata, subscriptions, metrics, metricGrpPrefix, time, retryBackoffMs, isolationLevel);
        this.maxPollRecords = maxPollRecords;
        return togoFetcher;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        acquire();
        try {
            Cluster cluster = this.metadata.fetch();
            List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
            if (!parts.isEmpty())
                return parts;

            Map<String, List<PartitionInfo>> topicMetadata = togoFetcher.getTopicMetadata(
                    new MetadataRequest.Builder(Collections.singletonList(topic), true)
                            .defaultConfigs(metaDataRequestProperties),
                    requestTimeoutMs);
            return topicMetadata.get(topic);
        } finally {
            release();
        }
    }

    void setFetchRule(TopicPartition topicPartition, FetchRule fetchRule) {
        togoFetcher.setFetchRule(topicPartition, fetchRule);
    }

    /**
     * determinate tag db table match use fnmatch or regex match
     */
    void setTagMatchFunction(TagMatchFunction tagMatchFunction) {
        togoFetcher.setTagMatchFunction(tagMatchFunction);
    }

    /**
     * add ClusterResourceListeners explicitly
     */
    void addClusterResourceListeners(ClusterResourceListener listener) {
        final String fieldName = "clusterResourceListeners";

        try {
            for (Field filed : Metadata.class.getDeclaredFields()) {
                if (fieldName.equals(filed.getName())) {
                    filed.setAccessible(true);
                    ((ClusterResourceListeners) filed.get(metadata)).maybeAdd(listener);
                    return;
                }
            }
        } catch (Exception foo) {
            // do nothing
        }

        throw new RuntimeException("Can not find field " + fieldName + " in Metadata class");
    }

    public Map<TopicPartition, OffsetAndQuery> offsetsForFirstMatch(Map<TopicPartition, QueryCondition> queriesToSearch) {
        return togoFetcher.retrieveOffsetsByQueries(queriesToSearch, requestTimeoutMs);
    }

    private void doFetchSchemas(Collection<SchemaRef> refs) {
        refs.forEach(ref -> {
            if (!pendingSchemas.containsKey(ref)) {
                TogoFuture<TogoFetcher.RawSchemaMetaData> future = togoFetcher.fetchSchema(ref)
                        .addCallback((schemaMetaData, throwable) -> {
                            completedSchemaFetches.add(new FetchSchemaResult(ref, schemaMetaData, throwable));
                        }, TogoFutures.directExecutor());
                pendingSchemas.put(ref, future);
            }
        });
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(long timeout) {
        throw new UnsupportedOperationException();
    }

    private DataDeserializer deserializer(String topic) {
        return deserializerMap
                .computeIfAbsent(topic, k ->
                        Data.get().deserializer(schemaCacheMap.computeIfAbsent(topic, t -> new LocalSchemaCache())));
    }

    private RuntimeException propagateThrowable(Throwable throwable) {
        if (throwable instanceof Error) {
            throw (Error) throwable;
        } else if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        } else {
            throw new RuntimeException(throwable);
        }
    }

    private void handleFetchedSchemas() {
        Set<String> affectedTopics = new HashSet<>();

        togoFetcher.handleUnsentFetchSchemaRequests();

        // add fetched metadata into local cache
        while (true) {
            FetchSchemaResult result = completedSchemaFetches.poll();
            if (result == null) {
                break;
            }
            if (result.getThrowable() != null) {
                throw propagateThrowable(result.getThrowable());
            }
            TogoFetcher.RawSchemaMetaData rawSchemaMetaData = result.getSchemaMetaData();
            if (pendingSchemas.remove(new SchemaRef(rawSchemaMetaData.getTopic(), rawSchemaMetaData.getId(), rawSchemaMetaData.getVersion())) == null) {
                logger.error("Illegal state: completed schema fetch not exists [{}]", rawSchemaMetaData);
            }
            SchemaMetaData schemaMetaData = new DefaultSchemaMetaData(rawSchemaMetaData.getTopic(), rawSchemaMetaData.getName(),
                    schemaDeserializer.deserialize(rawSchemaMetaData.getData()), rawSchemaMetaData.getId(),
                    rawSchemaMetaData.getVersion());
            schemaCacheMap
                    .computeIfAbsent(rawSchemaMetaData.getTopic(), t -> {
                        throw new IllegalStateException("SchemaCache for topic [" + rawSchemaMetaData.getTopic() + "] not exists");
                    })
                    .put(schemaMetaData);
            affectedTopics.add(rawSchemaMetaData.getTopic());
        }

        // use fetched schemas to deserialize pending messages
        affectedTopics.forEach(topic -> {
            DataDeserializer deserializer = deserializer(topic);
            Queue<PendingRecord> queue = pendingMessages.get(topic);
            // try to deserialize records until incurring the first missing schema
            while (true) {
                PendingRecord msg = queue.peek();
                if (msg == null) {
                    // the queue has been drained, removed it
                    pendingMessages.remove(topic);
                    break;
                }
                if (!msg.isResolved()) {
                    try {
                        msg.setSchemaAndValue(deserializer.deserialize(ByteBuffer.wrap(msg.getRawRecord().headers().lastHeader(TOGO_HEADER_KEY).value())));
                    } catch (SchemaNotFoundException e) {
                        logger.debug("Schema ({}, {}) for topic [{}] is still not available",
                                e.getId(), e.getVersion(), topic);
                        break;
                    }
                }
                // the message is deserialized successfully
                completedRecords.add(toSchemafulConsumerRecord(msg.getRawRecord(), msg.getSchemaAndValue()));
                queue.poll();
            }
        });
    }

    private Map<TopicPartition, List<SchemafulConsumerRecord>> pollCompletedRecords(int maxNum) {
        Map<TopicPartition, List<SchemafulConsumerRecord>> completedData = new HashMap<>();
        int polledNum = 0;
        while (polledNum < maxNum && !completedRecords.isEmpty()) {
            SchemafulConsumerRecord record = completedRecords.poll();
            completedData
                    .computeIfAbsent(new TopicPartition(record.topic(), record.partition()), k -> new ArrayList<>())
                    .add(record);
            polledNum++;
        }

        return completedData;
    }

    private Map<TopicPartition, List<SchemafulConsumerRecord>> pollSchemafulOnce(long timeout) {
        togoFetcher.maybeSendNewAndHandleCompletedFetchRules(timeout);

        handleFetchedSchemas();

        Map<TopicPartition, List<SchemafulConsumerRecord>> ret = pollCompletedRecords(maxPollRecords);

        if (!ret.isEmpty()) {
            return ret;
        }

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = pollOnce(timeout);
        Set<SchemaRef> unresolvedSchemas = new HashSet<>();

        records.forEach((tp, list) -> {
            DataDeserializer deserializer = deserializer(tp.topic());
            Queue<PendingRecord> pendingQueue = pendingMessages.get(tp.topic());
            List<SchemafulConsumerRecord> resolvedQueue = ret.get(tp);
            if (pendingQueue == null && resolvedQueue == null) {
                resolvedQueue = new ArrayList<>();
            }

            for (ConsumerRecord<byte[], byte[]> raw : list) {
                try {
                    SchemaAndValue schemaAndValue = deserializer.deserialize(ByteBuffer.wrap(raw.headers().lastHeader(TOGO_HEADER_KEY).value()));
                    if (pendingQueue == null) {
                        resolvedQueue.add(toSchemafulConsumerRecord(raw, schemaAndValue));
                    } else {
                        pendingQueue.add(new PendingRecord(raw, schemaAndValue));
                    }
                } catch (SchemaNotFoundException e) {
                    SchemaRef schemaRef = new SchemaRef(tp.topic(), e.getId(), e.getVersion());
                    if (!pendingSchemas.containsKey(schemaRef) && !unresolvedSchemas.contains(schemaRef)) {
                        unresolvedSchemas.add(schemaRef);
                    }

                    if (pendingQueue == null) {
                        pendingQueue = new ArrayDeque<>();
                    }
                    pendingQueue.add(new PendingRecord(raw));
                }
            }

            if (pendingQueue != null) {
                pendingMessages.put(tp.topic(), pendingQueue);
            }

            if (resolvedQueue != null && !resolvedQueue.isEmpty()) {
                ret.put(tp, resolvedQueue);
            }
        });

        // fetch schemas
        if (!unresolvedSchemas.isEmpty()) {
            logger.info("To fetch schemas for {}", unresolvedSchemas);
            doFetchSchemas(unresolvedSchemas);
        }

        // send requests
        togoFetcher.handleUnsentFetchSchemaRequests();

        return ret;
    }

    private SchemafulConsumerRecord toSchemafulConsumerRecord(ConsumerRecord<byte[], byte[]> raw, SchemaAndValue schemaAndValue) {
        return new SchemafulConsumerRecord(raw.topic(), raw.partition(), raw.offset(), raw.timestamp(),
                raw.timestampType(), raw.checksum(), raw.serializedValueSize(), schemaAndValue, raw.value());
    }

    SchemafulConsumerRecords pollSchemaful(long timeout) {
        acquire();
        try {
            if (timeout < 0)
                throw new IllegalArgumentException("Timeout must not be negative");

            if (this.subscriptions.hasNoSubscriptionOrUserAssignment())
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");

            long start = time.milliseconds();
            long remaining = timeout;
            do {
                Map<TopicPartition, List<SchemafulConsumerRecord>> records = pollSchemafulOnce(remaining);
                if (!records.isEmpty()) {
                    // before returning the fetched records, we can send off the next round of fetches
                    // and avoid block waiting for their responses to enable pipelining while the user
                    // is handling the fetched records.
                    //
                    // NOTE: since the consumed position has already been updated, we must not allow
                    // wakeups or any other errors to be triggered prior to returning the fetched records.
                    if (togoFetcher.sendFetches() > 0) {
                        client.pollNoWakeup();
                    }

                    return new SchemafulConsumerRecords(records);
                }

                long elapsed = time.milliseconds() - start;
                remaining = timeout - elapsed;
            } while (remaining > 0);

            return SchemafulConsumerRecords.empty();
        } finally {
            release();
        }
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        togoFetcher.maybeSendNewAndHandleCompletedFetchRules(requestTimeoutMs);
        return super.listTopics();
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        togoFetcher.maybeSendNewAndHandleCompletedFetchRules(requestTimeoutMs);
        return super.offsetsForTimes(timestampsToSearch);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        togoFetcher.maybeSendNewAndHandleCompletedFetchRules(requestTimeoutMs);
        super.seek(partition, offset);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        togoFetcher.maybeSendNewAndHandleCompletedFetchRules(requestTimeoutMs);
        super.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        togoFetcher.maybeSendNewAndHandleCompletedFetchRules(requestTimeoutMs);
        super.seekToEnd(partitions);
    }

    @Override
    public long position(TopicPartition partition) {
        togoFetcher.maybeSendNewAndHandleCompletedFetchRules(requestTimeoutMs);
        return super.position(partition);
    }

    static class PendingRecord {
        private final ConsumerRecord<byte[], byte[]> rawRecord;
        private SchemaAndValue schemaAndValue;

        public PendingRecord(ConsumerRecord<byte[], byte[]> rawRecord) {
            this(rawRecord, false, null);
        }

        public PendingRecord(ConsumerRecord<byte[], byte[]> rawRecord, SchemaAndValue schemaAndValue) {
            this(rawRecord, true, schemaAndValue);
        }

        public PendingRecord(ConsumerRecord<byte[], byte[]> rawRecord, boolean isResolved, SchemaAndValue schemaAndValue) {
            this.rawRecord = rawRecord;
            this.schemaAndValue = schemaAndValue;
        }

        public ConsumerRecord<byte[], byte[]> getRawRecord() {
            return rawRecord;
        }

        public boolean isResolved() {
            return schemaAndValue != null;
        }

        public SchemaAndValue getSchemaAndValue() {
            return schemaAndValue;
        }

        public void setSchemaAndValue(SchemaAndValue schemaAndValue) {
            this.schemaAndValue = schemaAndValue;
        }
    }

    static class FetchSchemaResult {
        private final SchemaRef schemaRef;
        private final TogoFetcher.RawSchemaMetaData schemaMetaData;
        private final Throwable throwable;

        public FetchSchemaResult(SchemaRef schemaRef, TogoFetcher.RawSchemaMetaData schemaMetaData, Throwable throwable) {
            this.schemaRef = schemaRef;
            this.schemaMetaData = schemaMetaData;
            this.throwable = throwable;
        }

        public SchemaRef getSchemaRef() {
            return schemaRef;
        }

        public TogoFetcher.RawSchemaMetaData getSchemaMetaData() {
            return schemaMetaData;
        }

        public Throwable getThrowable() {
            return throwable;
        }
    }
}
