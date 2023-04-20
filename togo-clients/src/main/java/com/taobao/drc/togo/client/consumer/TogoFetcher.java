package com.taobao.drc.togo.client.consumer;

import com.taobao.drc.togo.common.businesslogic.HashTypeFilterTag;
import com.taobao.drc.togo.common.businesslogic.SpecialTypeFilterTag;
import com.taobao.drc.togo.common.businesslogic.TagMatchFunction;
import com.taobao.drc.togo.common.businesslogic.UnitTypeFilterTag;
import com.taobao.drc.togo.util.concurrent.TogoFuture;
import com.taobao.drc.togo.util.concurrent.TogoFutures;
import com.taobao.drc.togo.util.concurrent.TogoPromise;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.internals.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.meta.RecordSchema;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author yangyang
 * @since 17/3/31
 */
public class TogoFetcher<K, V> extends Fetcher<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(TogoFetcher.class);

    private final Map<TopicPartition, FetchRule> pendingRules = new HashMap<>();
    private final Queue<FetchRuleResult> completedRules = new ConcurrentLinkedQueue<>();
    private final Map<TopicPartition, FetchRuleEntry> ruleMap = new HashMap<>();
    private final Set<Node> nodesNeedToRefreshFetchRules = new HashSet<>();
    private TagMatchFunction tagMatchFunction = TagMatchFunction.FNMATCH;

    // relaxing access privilege for unit tests
    final Map<String, Set<SchemaRef>> missingSchemas = new HashMap<>();
    final Map<SchemaRef, TogoPromise<RawSchemaMetaData>> pendingSchemas = new HashMap<>();
    final Queue<SchemaRef> unsentSchemas = new ArrayDeque<>();

    public TogoFetcher(LogContext logContext,
                       ConsumerNetworkClient client,
                       int minBytes,
                       int maxBytes,
                       int maxWaitMs,
                       int fetchSize,
                       int maxPollRecords,
                       boolean checkCrcs,
                       Deserializer<K> keyDeserializer,
                       Deserializer<V> valueDeserializer,
                       Metadata metadata,
                       SubscriptionState subscriptions,
                       Metrics metrics,
                       FetcherMetricsRegistry metricGrpPrefix,
                       Time time,
                       long retryBackoffMs,
                       IsolationLevel isolationLevel) {
        super(logContext, client, minBytes, maxBytes, maxWaitMs, fetchSize, maxPollRecords, checkCrcs, keyDeserializer,
                valueDeserializer, metadata, subscriptions, metrics, metricGrpPrefix, time, retryBackoffMs, isolationLevel);
    }

    @Override
    public void onAssignment(Set<TopicPartition> assignment) {
        super.onAssignment(assignment);
    }

    void setFetchRule(TopicPartition topicPartition, FetchRule fetchRule) {
        synchronized (this) {
            pendingRules.put(topicPartition, fetchRule);
        }
    }

    void setTagMatchFunction(TagMatchFunction tagMatchFunction) {
        this.tagMatchFunction = tagMatchFunction;
    }

    void maybeSendNewAndHandleCompletedFetchRules(long timeout) {
        handleCompletedFetchRules();

        handleNewFetchRules();

        handleDisconnectedConnections();

        handleUnCommittedRules();

        if (maybeSendFetchRules()) {
            client.poll(timeout);
        }

        handleCompletedFetchRules();
    }

    private void handleCompletedFetchRules() {
        while (true) {
            // iterate over the result queue
            FetchRuleResult result = completedRules.poll();
            if (result == null) {
                break;
            }
            FetchRuleEntry fetchRuleEntry = ruleMap.get(result.getTopicPartition());

            if (fetchRuleEntry == null) {
                // the rule entry should not be empty, since elements in the rule map are never removed
                logger.error("Illegal state: got set fetch rule result for [{}] without entry in rule map: [{}]",
                        result.getTopicPartition(), result);
                continue;
            }
            // the result is only valid when the version of the result equals to the version of the rule entry
            if (fetchRuleEntry.getVersion() == result.getVersion()) {
                if (result.isSuccess()) {
                    fetchRuleEntry.setState(FetchRuleState.COMMITTED);
                } else {
                    if (result.isRetryAble()) {
                        // if the response indicates
                        fetchRuleEntry.setState(FetchRuleState.INIT);
                    } else {
                        if (result.getException() != null) {
                            Exception exception = result.getException();
                            if (exception instanceof RuntimeException) {
                                throw (RuntimeException) exception;
                            } else {
                                throw new IllegalStateException(exception);
                            }
                        }
                    }
                }
            } else {
                logger.warn("Ignore the failed set fetch tag result that does not match the version of current fetch rule," +
                        "current fetch rule entry: [{}], fetch tag result: [{}]", fetchRuleEntry, result);
            }
        }
    }

    private void handleNewFetchRules() {
        // new rules to set
        Map<TopicPartition, FetchRule> localPendingRules;
        synchronized (this) {
            localPendingRules = new HashMap<>(pendingRules);
            pendingRules.clear();
        }
        localPendingRules.forEach((topicPartition, fetchRule) -> {
            ruleMap.compute(topicPartition, (tp, entry) -> {
                if (entry == null) {
                    logger.trace("To set the fetch rule for [{}]: [{}]", tp, fetchRule);
                    return new FetchRuleEntry(topicPartition, fetchRule);
                } else {
                    logger.trace("To update the fetch rule for [{}]: new [{}], old [{}]", tp, fetchRule, entry.getFetchRule());
                    if (entry.getFetchRule().equals(fetchRule)) {
                        logger.debug("Ignore the fetch rule that not changed for [{}]", tp);
                    } else {
                        logger.debug("Fetch rule for [{}] has changed to [{}]", tp, fetchRule);
                        entry.setFetchRule(fetchRule);
                        entry.setState(FetchRuleState.INIT);
                        entry.setVersion(entry.getVersion() + 1);
                    }
                    return entry;
                }
            });
        });
    }

    private void handleDisconnectedConnections() {
        // group topic-partitions by node
        Map<Node, List<FetchRuleEntry>> nodeToEntries = ruleMap.values().stream()
                .filter(entry -> entry.getNode() != null)
                .collect(Collectors.groupingBy(FetchRuleEntry::getNode));
        // if the connection to the node has failed, mark all topic-partitions as not committed
        nodeToEntries.forEach((node, entries) -> {
            if (client.connectionFailed(node)) {
                logger.warn("Previous connection to [{}] has been disconnected, re-set the fetch rules");
                entries.forEach(fetchRuleEntry -> {
                    fetchRuleEntry.setNode(null);
                    fetchRuleEntry.setVersion(fetchRuleEntry.getVersion() + 1);
                });
            }
        });
    }

    private void handleUnCommittedRules() {
        // assign nodes to each entry
        Cluster cluster = metadata.fetch();

        ruleMap.forEach((topicPartition, fetchRuleEntry) -> {
            Node leader = cluster.leaderFor(topicPartition);
            if (!Objects.equals(fetchRuleEntry.getNode(), leader)) {
                if (fetchRuleEntry.getNode() != null) {
                    nodesNeedToRefreshFetchRules.add(fetchRuleEntry.getNode());
                }
                fetchRuleEntry.setNode(leader);
                fetchRuleEntry.setState(FetchRuleState.INIT);
            }
            if (fetchRuleEntry.getNode() != null && fetchRuleEntry.getState() == FetchRuleState.INIT) {
                nodesNeedToRefreshFetchRules.add(fetchRuleEntry.getNode());
            }
        });
    }

    private <T> void checkTypeFilterNotExistAndPut(Map<TopicPartition, T> m, TopicPartition tp, T value) {
        m.compute(tp, (key, oldValue)->{
            if (null != oldValue) {
                throw new KafkaException("filter tag has existed for TopicPartition:" + tp.toString() +
                    ", existed value is " + oldValue.toString() +
                    ", new value is " + value.toString());
            }
            return value;
        });
    }

    private boolean maybeSendFetchRules() {
        AtomicBoolean hasSends = new AtomicBoolean(false);
        if (!nodesNeedToRefreshFetchRules.isEmpty()) {
            Map<Node, List<FetchRuleEntry>> entriesToRefreshMap = new HashMap<>();
            ruleMap.values().forEach(entry -> {
                if (entry.getNode() != null && nodesNeedToRefreshFetchRules.contains(entry.getNode())) {
                    entriesToRefreshMap.compute(entry.getNode(), (node, list) -> {
                        if (list == null) {
                            list = new ArrayList<>();
                        }
                        list.add(entry);
                        return list;
                    });
                }
            });
            nodesNeedToRefreshFetchRules.clear();

            entriesToRefreshMap.forEach((node, entries) -> {
                // the version map for future check after the response
                Map<TopicPartition, Integer> versionMap = new HashMap<>();
                // the tagList map
                Map<TopicPartition, List<SetFetchTagsRequest.TagEntry>> tagListMap = new HashMap<>();
                Map<TopicPartition, SpecialTypeFilterTag> specialTypeFilterTagMap = new HashMap<>();
                Map<TopicPartition, HashTypeFilterTag> hashTypeFilterTagMap = new HashMap<>();
                Map<TopicPartition, UnitTypeFilterTag> unitTypeFilterTagMap = new HashMap<>();
                entries.forEach(entry -> {
                    versionMap.put(entry.getTopicPartition(), entry.getVersion());
                    tagListMap.compute(entry.getTopicPartition(), (tp, list) -> {
                        if (list == null) {
                            list = new ArrayList<>();
                        }
                        list.addAll(entry.getFetchRule().createFetchEntry());
                        return list;
                    });
                    checkTypeFilterNotExistAndPut(specialTypeFilterTagMap, entry.getTopicPartition(), entry.getFetchRule().specialTypeFilterTag());
                    checkTypeFilterNotExistAndPut(hashTypeFilterTagMap, entry.getTopicPartition(), entry.getFetchRule().hashTypeFilterTag());
                    checkTypeFilterNotExistAndPut(unitTypeFilterTagMap, entry.getTopicPartition(), entry.getFetchRule().unitTypeFilterTag());
                    entry.setState(FetchRuleState.OUTSTANDING);
                });

                doSendSetFetchTagsRequest(node, tagListMap, specialTypeFilterTagMap, hashTypeFilterTagMap, unitTypeFilterTagMap)
                        .addListener(new RequestFutureListener<Map<TopicPartition, SetFetchTagsResponse.PartitionResponse>>() {
                            @Override
                            public void onSuccess(Map<TopicPartition, SetFetchTagsResponse.PartitionResponse> value) {
                                if (value.size() != entries.size()) {
                                    onFailure(new IllegalStateException("Invalid entry size in response, expected: [" + entries.size() + "]," +
                                            " actual: [" + value.size() + "]"));
                                } else {
                                    entries.forEach(entry -> {
                                        SetFetchTagsResponse.PartitionResponse partitionResp = value.get(entry.getTopicPartition());
                                        Integer version = versionMap.get(entry.getTopicPartition());
                                        Errors errors = Errors.forCode(partitionResp.errorCode);
                                        if (errors == Errors.NONE) {
                                            completedRules.add(new FetchRuleResult(entry.getTopicPartition(), entry.getFetchRule(),
                                                    version, true, null));
                                        } else {
                                            completedRules.add(new FetchRuleResult(entry.getTopicPartition(), entry.getFetchRule(),
                                                    version, false, errors.exception()));
                                        }
                                    });
                                }
                            }

                            @Override
                            public void onFailure(RuntimeException e) {
                                entries.forEach(entry -> {
                                    completedRules.add(new FetchRuleResult(entry.getTopicPartition(), entry.getFetchRule(),
                                            versionMap.get(entry.getTopicPartition()), false, e));
                                });
                            }
                        });
                hasSends.set(true);
            });
        }
        return hasSends.get();
    }

    private RequestFuture<Map<TopicPartition, SetFetchTagsResponse.PartitionResponse>> doSendSetFetchTagsRequest(
            Node node, Map<TopicPartition, List<SetFetchTagsRequest.TagEntry>> entryMap,
            Map<TopicPartition, SpecialTypeFilterTag> specialTypeFilterTagMap,
            Map<TopicPartition, HashTypeFilterTag> hashTypeFilterTagMap,
            Map<TopicPartition, UnitTypeFilterTag> unitTypeFilterTagMap) {
        SetFetchTagsRequest.Builder builder = new SetFetchTagsRequest.Builder(entryMap, hashTypeFilterTagMap, specialTypeFilterTagMap, unitTypeFilterTagMap);
        builder.setTagMatchFunction(tagMatchFunction.getFunctionCode());
        logger.debug("To set fetch tags on node {}: {}", node, entryMap);
        return client.send(node, builder)
                .compose(new RequestFutureAdapter<ClientResponse, Map<TopicPartition, SetFetchTagsResponse.PartitionResponse>>() {
                    @Override
                    public void onSuccess(ClientResponse value,
                                          RequestFuture<Map<TopicPartition, SetFetchTagsResponse.PartitionResponse>> future) {
                        handleSetFetchTagsResponse(value, future);
                    }
                });
    }

    private void handleSetFetchTagsResponse(ClientResponse response,
                                            RequestFuture<Map<TopicPartition, SetFetchTagsResponse.PartitionResponse>> future) {
        if (response.wasDisconnected()) {
            future.raise(Errors.NETWORK_EXCEPTION);
        } else {
            SetFetchTagsResponse setFetchTagsResponse = (SetFetchTagsResponse) response.responseBody();
            if (setFetchTagsResponse == null || setFetchTagsResponse.responseData() == null) {
                future.raise(new IllegalStateException("Invalid response for SetFetchTagsRequest: [" + response + "]"));
            } else {
                future.complete(setFetchTagsResponse.responseData());
            }
        }
    }

    @Override
    protected List<TopicPartition> fetchablePartitions() {
        List<TopicPartition> ret = super.fetchablePartitions();
        // make sure topics with missing schemas are not fetchable
        Set<String> missingSchemaTopics = missingSchemas.keySet();
        ret.removeIf(tp -> missingSchemaTopics.contains(tp.topic()));
        ret.removeIf(tp -> {
            FetchRuleEntry entry = ruleMap.get(tp);
            return entry != null && entry.getState() != FetchRuleState.COMMITTED;
        });
        return ret;
    }

    TogoFuture<RawSchemaMetaData> fetchSchema(SchemaRef ref) {
        Objects.requireNonNull(ref);
        TogoPromise<RawSchemaMetaData> promise = pendingSchemas.get(ref);
        if (promise == null) {
            promise = TogoFutures.promise();
            unsentSchemas.add(ref);
            pendingSchemas.put(ref, promise);
            missingSchemas.computeIfAbsent(ref.getTopic(), t -> new HashSet<>())
                    .add(ref);
        }
        return promise.future();
    }

    void handleUnsentFetchSchemaRequests() {
        // group refs on nodes
        Cluster cluster = metadata.fetch();

        // group unsent refs by topic
        Map<String, List<SchemaRef>> topicToRefs = unsentSchemas.stream().collect(Collectors.groupingBy(SchemaRef::getTopic));
        unsentSchemas.clear();

        // group topics of refs by nodes
        // 1. pick any available partition for a topic
        // 2. a node is only available for one topic
        Map<Node, List<SchemaRef>> nodeToRefs = new HashMap<>();
        for (Map.Entry<String, List<SchemaRef>> entry : topicToRefs.entrySet()) {
            // find an unique node for the topic to fetch the result
            String topic = entry.getKey();
            List<SchemaRef> refs = entry.getValue();

            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions == null || availablePartitions.isEmpty()) {
                logger.trace("No partitions are available for topic [{}]", topic);
                // mark the topic as stale
                metadata.add(topic);
                metadata.requestUpdate();
                continue;
            }

            // find any partition of the topic that is available
            Optional<Node> nodeOp = availablePartitions.stream()
                    .filter(p -> {
                        if (p.leader() != null) {
                            return true;
                        } else {
                            metadata.add(topic);
                            metadata.requestUpdate();
                            return false;
                        }
                    })
                    .filter(p -> !nodeToRefs.containsKey(p.leader()))
                    .findAny()
                    .map(PartitionInfo::leader);
            if (!nodeOp.isPresent()) {
                unsentSchemas.addAll(refs);
            } else {
                nodeToRefs.put(nodeOp.get(), refs);
            }
        }

        if (!nodeToRefs.isEmpty()) {
            makeAndSendFetchSchemaRequests(nodeToRefs);
        }
    }

    private void makeAndSendFetchSchemaRequests(Map<Node, List<SchemaRef>> nodeToRefs) {
        nodeToRefs.forEach((node, refs) -> {
            // the list should not be empty, and all refs in the list should have the same topic
            String topic = refs.get(0).getTopic();

            sendFetchSchemaRequest(node, topic, refs)
                    .addListener(new RequestFutureListener<List<RecordSchema>>() {
                        @Override
                        public void onSuccess(List<RecordSchema> metaDataList) {
                            if (metaDataList.size() != refs.size()) {
                                onFailure(new IllegalStateException("Number of requested schemas [" + refs.size() + "]" +
                                        " does not equal to the number of gotten schemas [" + metaDataList.size() + "]:" +
                                        " requested " + refs + ", gotten " + metaDataList.size()));
                                return;
                            }
                            for (int i = 0; i < refs.size(); i++) {
                                RecordSchema recordSchema = metaDataList.get(i);

                                Errors errors = recordSchema.getError();
                                if (errors == Errors.NONE) {
                                    completeFetchSchema(refs.get(i),
                                            new RawSchemaMetaData(topic, recordSchema.getName(), recordSchema.getId(),
                                                    recordSchema.getVersion(), recordSchema.getData()),
                                            null);
                                } else if (errors.exception() instanceof RetriableException) {
                                    unsentSchemas.add(refs.get(i));
                                } else {
                                    completeFetchSchema(refs.get(i), null, errors.exception());
                                }
                            }
                        }

                        @Override
                        public void onFailure(RuntimeException e) {
                            if (e instanceof RetriableException) {
                                unsentSchemas.addAll(refs);
                            } else {
                                refs.forEach(ref -> completeFetchSchema(ref, null, e));
                            }
                        }
                    });
        });
    }

    private RequestFuture<List<RecordSchema>> sendFetchSchemaRequest(final Node node, String topic,
                                                                     List<SchemaRef> refs) {
        FetchRecordMetasRequest.Builder builder = new FetchRecordMetasRequest.Builder(topic,
                new FetchRecordMetasRequest.RecordMetasToFetch(refs.stream()
                        .map(ref -> new FetchRecordMetasRequest.SchemaIDVersion(ref.getSchemaId(), (short) ref.getVersion()))
                        .collect(Collectors.toSet())), null, null);
        return client.send(node, builder)
                .compose(new RequestFutureAdapter<ClientResponse, List<RecordSchema>>() {
                    @Override
                    public void onSuccess(ClientResponse response, RequestFuture<List<RecordSchema>> future) {
                        if (response.wasDisconnected()) {
                            logger.error("Cancelled request {} due to node {} being disconnected", response, response.destination());
                            future.raise(Errors.NETWORK_EXCEPTION.exception());
                        } else {
                            FetchRecordMetasResponse fetchResponse = (FetchRecordMetasResponse) response.responseBody();
                            future.complete(fetchResponse.schemas());
                        }
                    }
                });
    }

    private void completeFetchSchema(SchemaRef ref, RawSchemaMetaData schemaMetaData, Throwable throwable) {
        TogoPromise<RawSchemaMetaData> promise = pendingSchemas.remove(ref);
        if (throwable != null) {
            promise.failure(throwable);
        } else {
            promise.success(schemaMetaData);
        }
        Set<SchemaRef> set = missingSchemas.get(ref.getTopic());
        set.remove(ref);
        if (set.isEmpty()) {
            missingSchemas.remove(ref.getTopic());
        }
    }

    Map<TopicPartition, OffsetAndQuery> retrieveOffsetsByQueries(Map<TopicPartition, QueryCondition> queriesToSearch,
                                                                 long timeout) {
        if (queriesToSearch.isEmpty()) {
            return Collections.emptyMap();
        }
        long startMs = time.milliseconds();
        long remaining = timeout;

        do {
            RequestFuture<Map<TopicPartition, OffsetAndQuery>> future = sendFlexListOffsetRequests(queriesToSearch);
            client.poll(future, remaining);

            if (!future.isDone())
                break;

            if (future.succeeded())
                return future.value();

            if (!future.isRetriable())
                throw future.exception();

            long elapsed = time.milliseconds() - startMs;
            remaining = timeout - elapsed;
            if (remaining <= 0)
                break;

            if (future.exception() instanceof InvalidMetadataException)
                client.awaitMetadataUpdate(remaining);
            else
                time.sleep(Math.min(remaining, retryBackoffMs));

            elapsed = time.milliseconds() - startMs;
            remaining = timeout - elapsed;
        } while (remaining > 0);
        throw new TimeoutException("Failed to get offsets by query conditions " + queriesToSearch + " in " + timeout + " ms");
    }

    private RequestFuture<Map<TopicPartition, OffsetAndQuery>> sendFlexListOffsetRequests(Map<TopicPartition, QueryCondition> queriesToSearch) {
        // Group the partitions by node.
        final Map<Node, Map<TopicPartition, QueryCondition>> queriesToSearchByNode = new HashMap<>();
        for (Map.Entry<TopicPartition, QueryCondition> entry : queriesToSearch.entrySet()) {
            TopicPartition tp = entry.getKey();
            PartitionInfo info = metadata.fetch().partition(tp);
            if (info == null) {
                metadata.add(tp.topic());
                logger.debug("Partition {} is unknown for fetching offset, wait for metadata refresh", tp);
                return RequestFuture.staleMetadata();
            } else if (info.leader() == null) {
                logger.debug("Leader for partition {} unavailable for fetching offset, wait for metadata refresh", tp);
                return RequestFuture.leaderNotAvailable();
            } else {
                queriesToSearchByNode
                        .computeIfAbsent(info.leader(), k -> new HashMap<>())
                        .put(entry.getKey(), entry.getValue());
            }
        }

        final RequestFuture<Map<TopicPartition, OffsetAndQuery>> listOffsetRequestsFuture = new RequestFuture<>();
        final Map<TopicPartition, OffsetAndQuery> fetchedMatchedOffsets = new HashMap<>();
        final AtomicInteger remainingResponses = new AtomicInteger(queriesToSearchByNode.size());
        for (Map.Entry<Node, Map<TopicPartition, QueryCondition>> entry : queriesToSearchByNode.entrySet()) {
            sendFlexListOffsetRequest(entry.getKey(), entry.getValue())
                    .addListener(new RequestFutureListener<Map<TopicPartition, OffsetAndQuery>>() {
                        @Override
                        public void onSuccess(Map<TopicPartition, OffsetAndQuery> value) {
                            synchronized (listOffsetRequestsFuture) {
                                fetchedMatchedOffsets.putAll(value);
                                if (remainingResponses.decrementAndGet() == 0 && !listOffsetRequestsFuture.isDone()) {
                                    listOffsetRequestsFuture.complete(fetchedMatchedOffsets);
                                }
                            }
                        }

                        @Override
                        public void onFailure(RuntimeException e) {
                            synchronized (listOffsetRequestsFuture) {
                                // This may cause all the requests to be retried, but should be rare.
                                if (!listOffsetRequestsFuture.isDone()) {
                                    listOffsetRequestsFuture.raise(e);
                                }
                            }
                        }
                    });
        }
        return listOffsetRequestsFuture;
    }

    private RequestFuture<Map<TopicPartition, OffsetAndQuery>> sendFlexListOffsetRequest(Node node,
                                                                                         Map<TopicPartition, QueryCondition> queries) {
        FlexListOffsetRequest.Builder builder = new FlexListOffsetRequest.Builder()
                .setTargetValues(queries.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().createRequestEntry())));
        return client.send(node, builder)
                .compose(new RequestFutureAdapter<ClientResponse, Map<TopicPartition, OffsetAndQuery>>() {
                    @Override
                    public void onSuccess(ClientResponse value, RequestFuture<Map<TopicPartition, OffsetAndQuery>> future) {
                        FlexListOffsetResponse response = (FlexListOffsetResponse) value.responseBody();
                        if (logger.isTraceEnabled()) {
                            logger.trace("Received FlexListOffsetResponse {} from broker {}", response, node);
                        }
                        handleFlexListOffsetResponse(queries, response, future);
                    }
                });
    }

    private void handleFlexListOffsetResponse(Map<TopicPartition, QueryCondition> queriesToSearch,
                                              FlexListOffsetResponse response,
                                              RequestFuture<Map<TopicPartition, OffsetAndQuery>> future) {
        Map<TopicPartition, OffsetAndQuery> queryOffsetMap = new HashMap<>();
        for (Map.Entry<TopicPartition, QueryCondition> entry : queriesToSearch.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            FlexListOffsetResponse.PartitionData partitionData = response.responseData().get(topicPartition);
            Errors error = Errors.forCode(partitionData.errorCode);
            if (error == Errors.NONE) {
                if (partitionData.offset != FlexListOffsetResponse.UNKNOWN_OFFSET) {
                    queryOffsetMap.put(topicPartition, new OffsetAndQuery(queriesToSearch.get(topicPartition), partitionData.offset));
                }
            } else if (error == Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT) {
                // The message format on the broker side is before 0.10.0, we simply put null in the response.
                logger.debug("Cannot search by timestamp for partition {} because the message format version " +
                        "is before 0.10.0", topicPartition);
                queryOffsetMap.put(topicPartition, null);
            } else if (error == Errors.NOT_LEADER_FOR_PARTITION) {
                logger.debug("Attempt to fetch offsets for partition {} failed due to obsolete leadership information, retrying.",
                        topicPartition);
                future.raise(error);
            } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                logger.warn("Received unknown topic or partition error in ListOffset request for partition {}. The topic/partition " +
                        "may not exist or the user may not have Describe access to it", topicPartition);
                future.raise(error);
            } else {
                logger.warn("Attempt to fetch offsets for partition {} failed due to: {}", topicPartition, error.message());
                future.raise(error.exception());
            }
        }
        if (!future.isDone())
            future.complete(queryOffsetMap);
    }

    enum FetchRuleState {
        INIT,
        OUTSTANDING,
        COMMITTED
    }

    static class FetchRuleEntry {
        private final TopicPartition topicPartition;
        private FetchRule fetchRule;
        private int version = 0;
        private Node node = null;
        private FetchRuleState state;

        public FetchRuleEntry(TopicPartition topicPartition, FetchRule fetchRule) {
            this.topicPartition = topicPartition;
            this.fetchRule = fetchRule;
        }

        public TopicPartition getTopicPartition() {
            return topicPartition;
        }

        public void setFetchRule(FetchRule fetchRule) {
            this.fetchRule = fetchRule;
        }

        public void setNode(Node node) {
            this.node = node;
        }

        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }

        public Node getNode() {
            return node;
        }

        public FetchRuleState getState() {
            return state;
        }

        public void setState(FetchRuleState state) {
            this.state = state;
        }

        public FetchRule getFetchRule() {
            return fetchRule;
        }

        @Override
        public String toString() {
            return "FetchRuleEntry{" +
                    "topicPartition=" + topicPartition +
                    ", fetchRule=" + fetchRule +
                    ", version=" + version +
                    ", node=" + node +
                    ", state=" + state +
                    '}';
        }
    }

    static class FetchRuleResult {
        private final TopicPartition topicPartition;
        private final FetchRule rule;
        private final Integer version;
        private final boolean success;
        private final Exception exception;

        public FetchRuleResult(TopicPartition topicPartition, FetchRule rule, Integer version, boolean success, Exception exception) {
            this.topicPartition = topicPartition;
            this.rule = rule;
            this.version = version;
            this.success = success;
            this.exception = exception;
        }

        public TopicPartition getTopicPartition() {
            return topicPartition;
        }

        public FetchRule getRule() {
            return rule;
        }

        public Integer getVersion() {
            return version;
        }

        public boolean isSuccess() {
            return success;
        }

        public boolean isRetryAble() {
            return exception != null && exception instanceof RetriableException;
        }

        public Exception getException() {
            return exception;
        }

        @Override
        public String toString() {
            return "FetchRuleResult{" +
                    "topicPartition=" + topicPartition +
                    ", rule=" + rule +
                    ", version=" + version +
                    ", success=" + success +
                    ", exception=" + exception +
                    '}';
        }
    }

    static class RawSchemaMetaData {
        private final String topic;
        private final String name;
        private final int id;
        private final int version;
        private final ByteBuffer data;

        public RawSchemaMetaData(String topic, String name, int id, int version, ByteBuffer data) {
            this.topic = topic;
            this.name = name;
            this.id = id;
            this.version = version;
            this.data = data;
        }

        public String getTopic() {
            return topic;
        }

        public String getName() {
            return name;
        }

        public int getId() {
            return id;
        }

        public int getVersion() {
            return version;
        }

        public ByteBuffer getData() {
            return data;
        }
    }
}
