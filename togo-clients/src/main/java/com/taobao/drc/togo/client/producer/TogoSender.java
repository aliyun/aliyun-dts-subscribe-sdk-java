package com.taobao.drc.togo.client.producer;

import com.taobao.drc.togo.data.Data;
import com.taobao.drc.togo.data.schema.Schema;
import com.taobao.drc.togo.data.schema.SchemaMetaData;
import com.taobao.drc.togo.data.schema.SchemaSerializer;
import com.taobao.drc.togo.data.schema.impl.DefaultSchemaMetaData;
import com.taobao.drc.togo.util.concurrent.TogoFuture;
import com.taobao.drc.togo.util.concurrent.TogoFutures;
import com.taobao.drc.togo.util.concurrent.TogoPromise;
import org.apache.kafka.clients.*;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.SenderMetricsRegistry;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.meta.RecordSchema;
import org.apache.kafka.common.requests.CreateRecordMetasRequest;
import org.apache.kafka.common.requests.CreateRecordMetasResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * @author yangyang
 * @since 17/3/31
 */
public class TogoSender extends Sender {
    private static final Logger logger = LoggerFactory.getLogger(TogoSender.class);

    private final Map<UnregisteredSchema, TogoPromise<SchemaMetaData>> pendingSchemas = new HashMap<>();
    private final Queue<UnregisteredSchema> unsentSchemas = new ConcurrentLinkedQueue<>();
    private final SchemaSerializer schemaSerializer = Data.get().schemaSerializer();
    private boolean isClosed = false;

    public TogoSender(LogContext logContext, KafkaClient client, Metadata metadata, RecordAccumulator accumulator, boolean guaranteeMessageOrder,
                      int maxRequestSize, short acks, int retries, SenderMetricsRegistry metrics, Time time, int requestTimeout,
                      long retryBackoffMs, TransactionManager transactionManager, ApiVersions apiVersions) {

        super(logContext, client, metadata, accumulator, guaranteeMessageOrder, maxRequestSize, acks,
                retries, metrics, time, requestTimeout, retryBackoffMs, transactionManager, apiVersions);
    }

    TogoFuture<SchemaMetaData> registerSchema(String topic, String name, Schema schema) {
        UnregisteredSchema ref = new UnregisteredSchema(topic, name, schema, schemaSerializer.serialize(schema), retries, time.milliseconds());
        synchronized (this) {
            if (isClosed) {
                return TogoFutures.failure(new IllegalStateException("Sender has already been closed"));
            }
            TogoPromise<SchemaMetaData> promise = pendingSchemas.get(ref);
            if (promise == null) {
                promise = TogoFutures.promise();
                pendingSchemas.put(ref, promise);
                unsentSchemas.add(ref);

                wakeup();
            }
            return promise.future();
        }
    }

    private List<UnregisteredSchema> drainUnregisteredSchemas() {
        List<UnregisteredSchema> ret = new ArrayList<>();
        while (true) {
            UnregisteredSchema ref = unsentSchemas.poll();
            if (ref == null) {
                break;
            }
            ret.add(ref);
        }
        return ret;
    }

    private void maybeSendRegisterSchemasRequest(long now) {
        List<UnregisteredSchema> localUnsent = drainUnregisteredSchemas();
        if (localUnsent.isEmpty()) {
            return;
        }

        Cluster cluster = metadata.fetch();
        List<UnregisteredSchema> aborted = localUnsent.stream()
                // deal with timeout requests
                .filter(ref -> maybeAbortTimeoutRegisterRequest(ref, now))
                .collect(Collectors.toList());
        aborted.forEach(pendingSchemas::remove);

        Map<String, List<UnregisteredSchema>> topicToRefs = localUnsent.stream()
                .filter(ref -> !ref.isClosed())
                .collect(Collectors.groupingBy(UnregisteredSchema::getTopic));

        Map<Node, List<UnregisteredSchema>> nodeToRefs = new HashMap<>();

        for (Map.Entry<String, List<UnregisteredSchema>> entry : topicToRefs.entrySet()) {
            String topic = entry.getKey();
            List<UnregisteredSchema> refs = entry.getValue();

            List<PartitionInfo> availables = cluster.availablePartitionsForTopic(topic);
            if (availables != null) {
                Node node = availables.stream()
                        .filter(p -> p.leader() != null && !nodeToRefs.containsKey(p.leader()))
                        .map(PartitionInfo::leader)
                        .findAny()
                        .orElse(null);
                logger.debug("No available partitions for topic [{}] to register schema", topic);
                if (node != null) {
                    if (client.ready(node, now)) {
                        nodeToRefs.put(node, refs);
                    } else {
                        logger.debug("Connection to [{}] is not ready", node);
                        unsentSchemas.addAll(refs);
                    }
                    continue;
                }
            }
            // missing meta data, fetch later
            metadata.add(topic);
            metadata.requestUpdate();
            unsentSchemas.addAll(refs);
        }

        nodeToRefs.forEach((node, unregisteredSchemas) -> maybeSendRegisterSchemasRequest(now, node, unregisteredSchemas));
        wakeup();
    }

    private void maybeAbortTimeoutRegisterRequests(long now) {
        synchronized (this) {
            if (!pendingSchemas.isEmpty()) {
                List<UnregisteredSchema> aborted = pendingSchemas.keySet().stream()
                        .filter(ref -> maybeAbortTimeoutRegisterRequest(ref, now))
                        .collect(Collectors.toList());
                aborted.forEach(pendingSchemas::remove);
            }
        }
    }

    private boolean maybeAbortTimeoutRegisterRequest(UnregisteredSchema ref, long now) {
        if (now - ref.getCreatedMs() < requestTimeout) {
            return false;
        }
        synchronized (this) {
            TogoPromise<SchemaMetaData> future = pendingSchemas.get(ref);
            if (future == null) {
                logger.warn("Illegal state: future for [{}] not exists", ref);
            } else {
                future.failure(Errors.REQUEST_TIMED_OUT.exception());
            }
            ref.close();
        }
        return true;
    }

    private void maybeSendRegisterSchemasRequest(long now, Node node, List<UnregisteredSchema> refs) {
        if (refs.isEmpty()) {
            return;
        }
        logger.debug("To register schemas on node [{}]: [{}]", node, refs);
        CreateRecordMetasRequest.Builder builder = new CreateRecordMetasRequest.Builder(refs.get(0).getTopic(),
                refs.stream()
                        .map(ref -> new RecordSchema(ref.name, ByteBuffer.wrap(ref.getEncodedSchema())))
                        .collect(Collectors.toList()), null, null);
        ClientRequest request = client.newClientRequest(node.idString(), builder, now, true,
                response -> {
                    handleRegisterSchemaResponse(refs, response);
                });
        client.send(request, now);
    }

    private void handleRegisterSchemaResponse(List<UnregisteredSchema> refs, ClientResponse response) {
        long now = time.milliseconds();
        if (response.wasDisconnected()) {
            logger.warn("Cancelled request {} due to node {} being disconnected", response, response.destination());
            refs.forEach(ref -> completeRegisterSchemaRequest(ref, null, Errors.NETWORK_EXCEPTION.exception(), now));
        } else if (!response.hasResponse()) {
            logger.error("Destination server {} does not support CreateRecordMetas request! response: {}",
                    response.destination(), response);
            refs.forEach(ref -> completeRegisterSchemaRequest(ref, null, Errors.UNSUPPORTED_VERSION.exception(), now));
        } else {
            try {
                CreateRecordMetasResponse casted = (CreateRecordMetasResponse) response.responseBody();

                if (refs.size() != casted.schemaErrors().size()) {
                    throw new IllegalStateException("Invalid schema [" + casted + "]," +
                            " pending requests size [{" + refs.size() + "}] != response size [" + casted.schemaErrors().size() + "]");
                }
                for (int i = 0; i < refs.size(); i++) {
                    UnregisteredSchema ref = refs.get(i);
                    RecordSchema schemaError = casted.schemaErrors().get(i);
                    completeRegisterSchemaRequest(ref, schemaError, schemaError.getError().exception(), now);
                }
            } catch (Exception e) {
                logger.error("Failed to parse fetch schema response {} for requests {}", response, refs, e);
                refs.forEach(ref -> completeRegisterSchemaRequest(ref, null, e, now));
            }
        }
    }

    private void completeRegisterSchemaRequest(UnregisteredSchema ref, RecordSchema recordSchema, Exception exception, long now) {

        if (exception != null && canRetry(ref, exception, now)) {
            // add the entry to the queue of unsent schemas again
            ref.setRetry();
            unsentSchemas.add(ref);
            return;
        }
        TogoPromise<SchemaMetaData> promise;
        synchronized (this) {
            promise = pendingSchemas.remove(ref);
        }
        if (promise == null) {
            logger.debug("Illegal state: ref {} is not in the pending map, the previous request may incurred a timeout", ref);
            return;
        }
        if (exception != null) {
            promise.failure(exception);
        } else {
            promise.success(new DefaultSchemaMetaData(ref.getTopic(), ref.getName(), ref.getSchema(),
                    recordSchema.getId(), (int) recordSchema.getVersion()));
        }
        ref.close();
    }

    private boolean canRetry(UnregisteredSchema ref, Exception exception, long now) {
        return ref.getAttempts() < ref.getMaxTries() && exception instanceof RetriableException;
    }

    @Override
    protected void run(long now) {
        maybeSendRegisterSchemasRequest(now);
        maybeAbortTimeoutRegisterRequests(now);
        super.run(now);
    }

    @Override
    public void forceClose() {
        super.forceClose();
        synchronized (this) {
            isClosed = true;
            pendingSchemas.values().forEach(future -> future.failure(new InterruptedException("Sender was closed")));
        }
    }

    static class UnregisteredSchema implements Closeable {
        private final String topic;
        private final String name;
        private final Schema schema;
        private final byte[] encodedSchema;
        private final int maxTries;
        private final long createdMs;
        private boolean inRetry = false;
        private int attempts = 0;
        private boolean closed = false;

        UnregisteredSchema(String topic, String name, Schema schema, byte[] encodedSchema, int maxTries, long createdMs) {
            this.topic = topic;
            this.name = name;
            this.schema = schema;
            this.encodedSchema = encodedSchema;
            this.maxTries = maxTries;
            this.createdMs = createdMs;
        }

        public String getTopic() {
            return topic;
        }

        public String getName() {
            return name;
        }

        public Schema getSchema() {
            return schema;
        }

        public byte[] getEncodedSchema() {
            return encodedSchema;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UnregisteredSchema that = (UnregisteredSchema) o;
            return Objects.equals(topic, that.topic) &&
                    Objects.equals(name, that.name) &&
                    Objects.equals(schema, that.schema);
        }

        public void setRetry() {
            this.inRetry = true;
            this.attempts++;
        }

        public int getMaxTries() {
            return maxTries;
        }

        public long getCreatedMs() {
            return createdMs;
        }

        public int getAttempts() {
            return attempts;
        }

        public boolean inRetry() {
            return inRetry;
        }

        @Override
        public void close() {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, name, schema, encodedSchema, maxTries, createdMs, inRetry, attempts, closed);
        }

        @Override
        public String toString() {
            return "UnregisteredSchema{" +
                    "topic='" + topic + '\'' +
                    ", name='" + name + '\'' +
                    ", schema=" + schema +
                    ", maxTries=" + maxTries +
                    ", createdMs=" + createdMs +
                    ", inRetry=" + inRetry +
                    ", attempts=" + attempts +
                    ", closed=" + closed +
                    '}';
        }
    }
}
