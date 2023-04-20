package com.taobao.drc.togo.client.producer;

import com.taobao.drc.togo.data.Data;
import com.taobao.drc.togo.data.DataSerializer;
import com.taobao.drc.togo.data.schema.Schema;
import com.taobao.drc.togo.data.schema.SchemaBuilder;
import com.taobao.drc.togo.data.schema.SchemaMetaData;
import com.taobao.drc.togo.util.concurrent.TogoCallback;
import com.taobao.drc.togo.util.concurrent.TogoFuture;
import com.taobao.drc.togo.util.concurrent.TogoFutures;
import com.taobao.drc.togo.util.concurrent.TogoPromise;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.SenderMetricsRegistry;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.taobao.drc.togo.util.CommonName.TOGO_HEADER_KEY;

/**
 * A Togo client that publishes schemaful records to the Togo cluster.
 * <p>
 * A schemaful record is a record the structure of which is defined by a specific schema. The schema is a topic-wide
 * meta data that must be registered with {@link #registerSchema(String, String, Schema)} on the broker before any
 * record conforms the schema is sent.
 * <p>
 * A schema can be built with the {@link SchemaBuilder}. Common primitive types, array, map and nested structurs are
 * supported. For example, a schema can be defined as follows:
 * <p>
 * <pre>
 * {@code
 *
 * Schema schema = SchemaBuilder.struct().name("testSchema")
 *     .field("bool", bool())
 *     .field("int", int32())
 *     .field("long", int64())
 *     .field("float", float32())
 *     .field("double", float64())
 *     .field("bytes", bytes())
 *     .field("string", string())
 *     .field("map", map(string()))
 *     .field("array", array(struct().field("int", int32()).field("string", string()).build()))
 *     .build();
 * }
 * </pre>
 * <p>
 * A schemaful record conforms the record can be constructed as:
 * <p>
 * <pre>
 * {@code
 *
 * SchemafulRecord record = Data.get().newRecord(schema)
 *     .put("bool", true)
 *     .put("int", 1)
 *     .put("long", 2L)
 *     .put("float", 1.2f)
 *     .put("double", 1.2)
 *     .put("bytes", "foo".getBytes())
 *     .put("string", "foo")
 *     .put("map", Collections.singletonMap("key", "string"))
 *     .put("array", Arrays.asList(
 *         Data.get().newRecord(schema.field("array").schema().valueSchema())
 *             .put("int", 123)
 *             .put("string", "nested")
 *         )
 *     );
 * }
 * </pre>
 * <p>
 * Here is a simple example to publish schemaful records using the schema defined above to the cluster, suppose we have
 * already constructed several schemaful records and put them in a list called {@code records}.
 * <p>
 * <pre>
 * {@code
 *
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("acks", "all");
 * props.put("retries", 0);
 * props.put("batch.size", 16384);
 * props.put("linger.ms", 1);
 * props.put("buffer.memory", 33554432);
 *
 * SchemafulProducer producer = new TogoProducer(props);
 *
 * SchemaMetaData schemaMetaData = producer.registerSchema(schema).get();
 *
 * records.forEach(record -> {
 *     producer.send(new SchemafulProducerRecord("test", 0, record, schemaMetaData));
 * });
 *
 * producer.close();
 * }
 * </pre>
 * <p>
 * The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server
 * as well as a background I/O thread that is responsible for turning these records into requests and transmitting them
 * to the cluster. Failure to close the producer after use will leak these resources.
 * <p>
 * The {@link #send(SchemafulProducerRecord) send()} method is asynchronous. When called it adds the record to a buffer of pending record sends
 * and immediately returns. This allows the producer to batch together individual records for efficiency.
 * <p>
 * The <code>acks</code> config controls the criteria under which requests are considered complete. The "all" setting
 * we have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
 * <p>
 * If the request fails, the producer can automatically retry, though since we have specified <code>retries</code>
 * as 0 it won't. Enabling retries also opens up the possibility of duplicates (see the documentation on
 * <a href="http://kafka.apache.org/documentation.html#semantics">message delivery semantics</a> for details).
 * <p>
 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by
 * the <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we will
 * generally have one of these buffers for each active partition).
 * <p>
 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However if you
 * want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This will
 * instruct the producer to wait up to that number of milliseconds before sending a request in hope that more records will
 * arrive to fill up the same batch. This is analogous to Nagle's algorithm in TCP. For example, in the code snippet above,
 * likely all 100 records would be sent in a single request since we set our linger time to 1 millisecond. However this setting
 * would add 1 millisecond of latency to our request waiting for more records to arrive if we didn't fill up the buffer. Note that
 * records that arrive close together in time will generally batch together even with <code>linger.ms=0</code> so under heavy load
 * batching will occur regardless of the linger configuration; however setting this to something larger than 0 can lead to fewer, more
 * efficient requests when not under maximal load at the cost of a small amount of latency.
 * <p>
 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it throws
 * a TimeoutException.
 */
public class TogoProducer implements SchemafulProducer {
    private static final Logger logger = LoggerFactory.getLogger(TogoProducer.class);

    private final TogoAdaptedKafkaProducer<byte[], byte[]> producer;
    private final DataSerializer serializer;
    private final SchemafulProducerInterceptors interceptors;
    public TogoProducer(Map<String, Object> configs) {
        this(new SchemafulProducerConfig(configs));
    }

    public TogoProducer(Properties properties) {
        this(new SchemafulProducerConfig(properties));
    }

    private TogoProducer(SchemafulProducerConfig config) {
        Map<String, Object> originals = config.originals();
        serializer = Data.get().serializer(config.getBoolean(SchemafulProducerConfig.RECORD_SERIALIZER_FIELD_OFFSET_CONFIG));
        String interceptClasses = (String) originals.get(SchemafulProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
        originals.remove(SchemafulProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
        this.producer = new TogoAdaptedKafkaProducer<>(preProcessProperties(originals));

        if (interceptClasses != null) {
            originals.put(SchemafulProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptClasses);
            originals.put(SchemafulProducerConfig.CLIENT_ID_CONFIG, this.producer.getClientId());
            List<SchemafulProducerInterceptor> interceptorList = new SchemafulProducerConfig(originals, false)
                    .getConfiguredInstances(SchemafulProducerConfig.INTERCEPTOR_CLASSES_CONFIG, SchemafulProducerInterceptor.class);
            this.interceptors = interceptorList.isEmpty() ? null : new SchemafulProducerInterceptors(interceptorList);
        } else {
            this.interceptors = null;
        }
    }

    private Map<String, Object> preProcessProperties(Map<String, Object> configs) {
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return configs;
    }

    @Override
    public TogoFuture<SchemaMetaData> registerSchema(String topic, String name, Schema schema) {
        producer.mayClusterSwitch();
        return producer.registerSchema(topic, name, schema);
    }

    @Override
    public TogoFuture<RecordMetadata> send(SchemafulProducerRecord record) {
        return send(record, null);
    }

    /**
     * Asynchronously send a record to a topic. Callbacks can be added to the returned future to be invoked when the
     * send has been acknowledged.
     * <p>
     * The send is asynchronous and this method will return immediately once the record has been stored in the buffer of
     * records waiting to be sent. This allows sending many records in parallel without blocking to wait for the
     * response after each one.
     * <p>
     * The result of the send is a {@link RecordMetadata} specifying the partition the record was sent to, the offset
     * it was assigned and the timestamp of the record. If
     * {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime} is used by the topic, the timestamp
     * will be the user provided timestamp or the record send time if the user did not specify a timestamp for the
     * record. If {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime} is used for the
     * topic, the timestamp will be the Kafka broker local time when the message is appended.
     * <p>
     * Since the send call is asynchronous it returns a {@link java.util.concurrent.Future Future} for the
     * {@link RecordMetadata} that will be assigned to this record. Invoking {@link java.util.concurrent.Future#get()
     * get()} on this future will block until the associated request completes and then return the metadata for the record
     * or throw any exception that occurred while sending the record.
     * <p>
     * If you want to simulate a simple blocking call you can call the <code>get()</code> method immediately:
     * <p>
     * <pre>
     * {@code
     *
     * SchemafulRecord record = ... // created by user
     * SchemaMetaData schemaMetaData = ... // defined by user and registered to the broker
     * SchemafulProducerRecord record = new SchemafulProducerRecord("my-topic", record, schemaMetaData);
     * producer.send(record).get();
     * }</pre>
     * <p>
     * Fully non-blocking usage can make use of the callback that will be invoked when the request is complete.
     * <p>
     * <pre>
     * {@code
     *
     * SchemafulProducerRecord record = new SchemafulProducerRecord("the-topic", record, schemaMetaData);
     * producer.send(myRecord)
     *     .addCallback((metadata, e) -> {
     *                   if(e != null) {
     *                      e.printStackTrace();
     *                   } else {
     *                      System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *                   }
     *               });
     * }
     * </pre>
     * <p>
     * Callbacks for records being sent to the same partition are guaranteed to execute in order. That is, in the
     * following example <code>callback1</code> is guaranteed to execute before <code>callback2</code>:
     * <p>
     * <pre>
     * {@code
     *
     * producer.send(new SchemafulProducerRecord(topic, partition, value1, schemaMetaData1).addCallback(callback1);
     * producer.send(new SchemafulProducerRecord(topic, partition, value2, schemaMetaData2).addCallback(callback2);
     * }
     * </pre>
     * <p>
     * Note that callbacks will generally execute in the I/O thread of the producer and so should be reasonably fast or
     * they will delay the sending of messages from other threads. If you want to execute blocking or computationally
     * expensive callbacks it is recommended to use your own {@link java.util.concurrent.Executor} in the callback body
     * to parallelize processing.
     *
     * @param record The record to send
     */
    @Override
    public TogoFuture<RecordMetadata> send(SchemafulProducerRecord record, TogoCallback<RecordMetadata> callback) {
        SchemafulProducerRecord interceptedRecord = interceptors == null ? record : interceptors.onSend(record);
        return doSend(interceptedRecord, callback);
    }

    private TogoFuture<RecordMetadata> doSend(SchemafulProducerRecord record, TogoCallback<RecordMetadata> callback) {
        TogoPromise<RecordMetadata> promise = TogoFutures.promise();
        TogoFuture<RecordMetadata> future = promise.future();
        TopicPartition tp = null;

        if (null != callback) {
            future.addCallback(callback);
        }

        try {
            byte[] header = serializer.serialize(record.value(), record.schemaMetaData());
            tp = new TopicPartition(record.topic(), record.partition());
            final TopicPartition finalTp = tp;

            producer.send(
                    new ProducerRecord<>(record.topic(), record.partition(),
                            record.timestamp(), null, record.data(), new RecordHeaders(Arrays.asList(new RecordHeader(TOGO_HEADER_KEY, header)))),
                    (metadata, exception) -> {
                        if (this.interceptors != null) {
                            try {
                                if (metadata == null) {
                                    this.interceptors.onAcknowledgement(new RecordMetadata(finalTp, -1, -1, RecordBatch.NO_TIMESTAMP, -1, -1, -1),
                                            exception);
                                } else {
                                    this.interceptors.onAcknowledgement(metadata, exception);
                                }
                            } catch (Exception e) {
                                logger.error("Caught error on acknowledgement to interceptors");
                            }
                        }
                        if (exception != null) {
                            promise.failure(exception);
                        } else {
                            promise.success(metadata);
                        }
                    });
        } catch (Exception e) {
            if (interceptors != null) {
                interceptors.onSendError(record, tp, e);
            }
            throw e;
        }

        return future;
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
     * greater than 0) and blocks on the completion of the requests associated with these records. The post-condition
     * of <code>flush()</code> is that any previously sent record will have completed.
     * A request is considered completed when it is successfully acknowledged
     * according to the <code>acks</code> configuration you have specified or else it results in an error.
     * <p>
     * Other threads can continue sending records while one thread is blocked waiting for a flush call to complete,
     * however no guarantee is made about the completion of records sent after the flush call begins.
     * <p>
     * This method can be useful when consuming from some input system and producing into Togo. The <code>flush()</code> call
     * gives a convenient way to ensure all previously sent messages have actually completed.
     * <p>
     * This example shows how to consume from one Togo topic and produce to another Togo topic:
     * <pre>
     * {@code
     * for(SchemafulConsumerRecord record: consumer.poll(100))
     *     producer.send(new SchemafulProducerRecord("my-topic", record.value(), schemaMetaData);
     * producer.flush();
     * consumer.commit();
     * }
     * </pre>
     * <p>
     * Note that the above example may drop records if the produce request fails. If we want to ensure that this does not occur
     * we need to set <code>retries=&lt;large_number&gt;</code> in our config.
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void flush() {
        producer.flush();
    }

    /**
     * Get the partition metadata for the give topic. This can be used for custom partitioning.
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return producer.partitionsFor(topic);
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return producer.metrics();
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * This method is equivalent to <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     * <p>
     * <strong>If close() is called from {@link Callback}, a warning message will be logged and close(0, TimeUnit.MILLISECONDS)
     * will be called instead. We do this because the sender thread would otherwise try to join itself and
     * block forever.</strong>
     * <p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void close() {
        producer.close();
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail
     * any unsent and unacknowledged records immediately.
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(0, TimeUnit.MILLISECONDS)</code>. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests. The value should be
     *                non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
     * @param unit    The time unit for the <code>timeout</code>
     * @throws InterruptException       If the thread is interrupted while blocked
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     */
    @Override
    public void close(long timeout, TimeUnit unit) {
        producer.close(timeout, unit);
    }

    public boolean setOffsetForTopicPartition(Long offset, SchemafulProducerRecord record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        if (!producer.setOffsetForTopicPartition(tp, offset)) {
            return false;
        }
        try {
            send(record).get();
            return true;
        } catch (Exception e) {
            logger.info("set offset for {} failed cause: {}", tp, e.getMessage());
            return false;
        }
    }

    static class TogoAdaptedKafkaProducer<K, V> extends KafkaProducer<K, V> {
        private static final String LEADER_SWITCH_INTERRUPT_LISTENER_CONFIG_NAME = "leaderSwitchInterrupt";
        private static final List<String> metaDataRequestProperties = Collections.singletonList("storage.engine.type=togo");
        private TogoSender togoSender;
        private volatile boolean leaderSwitched;
        private volatile String switchMessage;

        TogoAdaptedKafkaProducer(Map<String, Object> properties) {
            super(properties);
            leaderSwitched = false;
            switchMessage = null;
            mayAddMetaDataLeaderChangeListener(properties);
        }

        TogoFuture<SchemaMetaData> registerSchema(String topic, String name, Schema schema) {
            return togoSender.registerSchema(topic, name, schema);
        }

        String getClientId() {
            return clientId;
        }

        @Override
        protected NetworkClient createClient(Selectable selector, Metadata metadata, String clientId, int maxInFlightRequestsPerConnection,
                                             long reconnectBackoffMs, long reconnectBackoffMax, int socketSendBuffer, int socketReceiveBuffer, int requestTimeoutMs,
                                             Time time, boolean discoverBrokerVersions, ApiVersions apiVersions, Sensor throttleTimeSensor,
                                             LogContext logContext) {
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
        protected Sender createSender(LogContext logContext,
                                      KafkaClient client,
                                      Metadata metadata,
                                      RecordAccumulator accumulator,
                                      boolean guaranteeMessageOrder,
                                      int maxRequestSize,
                                      short acks,
                                      int retries,
                                      SenderMetricsRegistry metrics,
                                      Time time,
                                      int requestTimeout,
                                      long retryBackoffMs,
                                      TransactionManager transactionManager,
                                      ApiVersions apiVersions) {
            togoSender = new TogoSender(logContext, client, metadata, accumulator, guaranteeMessageOrder, maxRequestSize, acks, retries,
                    metrics, time, requestTimeout, retryBackoffMs, transactionManager, apiVersions);
            return togoSender;
        }

        protected boolean setOffsetForTopicPartition(TopicPartition tp, Long offset) {
            return accumulator.setOffsetForTP(tp, offset);
        }
        private void mayAddMetaDataLeaderChangeListener(Map<String, Object> properties) {
            Object configValue = properties.get(LEADER_SWITCH_INTERRUPT_LISTENER_CONFIG_NAME);
            if (null != configValue) {
                if (String.valueOf(configValue).equals("true")) {
                    metadata.addListener(new LeaderSwitchInterruptListener(this));
                }
            }
        }
        public void notifyLeaderSwitch(String message) {
            switchMessage = message;
            leaderSwitched = true;
            logger.info("Leader switch interrupted notify to listener, message [{}]", message);
        }
        public void mayClusterSwitch() {
            if (true == leaderSwitched) {
                throw new LeaderSwitchInterruptListener.LeaderSwitchException(switchMessage);
            }
        }
    }
}
