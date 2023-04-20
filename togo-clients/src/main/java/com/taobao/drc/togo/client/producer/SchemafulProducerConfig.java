package com.taobao.drc.togo.client.producer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * @author yangyang
 * @since 17/4/19
 */
public class SchemafulProducerConfig extends AbstractConfig {
     /*
     * NOTE: DO NOT CHANGE EITHER CONFIG STRINGS OR THEIR JAVA VARIABLE NAMES AS THESE ARE PART OF THE PUBLIC API AND
     * CHANGE WILL BREAK USER CODE.
     */

    private static final ConfigDef CONFIG;

    /**
     * <code>bootstrap.servers</code>
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

    /** <code>metadata.fetch.timeout.ms</code> */
    /**
     * @deprecated This config will be removed in a future release. Please use {@link #MAX_BLOCK_MS_CONFIG}
     */
    @Deprecated
    public static final String METADATA_FETCH_TIMEOUT_CONFIG = "metadata.fetch.timeout.ms";
    private static final String METADATA_FETCH_TIMEOUT_DOC = "The first time data is sent to a topic we must fetch metadata about that topic to know which servers "
            + "host the topic's partitions. This config specifies the maximum time, in milliseconds, for this fetch "
            + "to succeed before throwing an exception back to the client.";

    /**
     * <code>metadata.max.age.ms</code>
     */
    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
    private static final String METADATA_MAX_AGE_DOC = CommonClientConfigs.METADATA_MAX_AGE_DOC;

    /**
     * <code>batch.size</code>
     */
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC = "The producer will attempt to batch records together into fewer requests whenever multiple records are being sent"
            + " to the same partition. This helps performance on both the client and the server. This configuration controls the "
            + "default batch size in bytes. "
            + "<p>"
            + "No attempt will be made to batch records larger than this size. "
            + "<p>"
            + "Requests sent to brokers will contain multiple batches, one for each partition with data available to be sent. "
            + "<p>"
            + "A small batch size will make batching less common and may reduce throughput (a batch size of zero will disable "
            + "batching entirely). A very large batch size may use memory a bit more wastefully as we will always allocate a "
            + "buffer of the specified batch size in anticipation of additional records.";

    /**
     * <code>acks</code>
     */
    public static final String ACKS_CONFIG = "acks";
    private static final String ACKS_DOC = "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the "
            + " durability of records that are sent. The following settings are allowed: "
            + " <ul>"
            + " <li><code>acks=0</code> If set to zero then the producer will not wait for any acknowledgment from the"
            + " server at all. The record will be immediately added to the socket buffer and considered sent. No guarantee can be"
            + " made that the server has received the record in this case, and the <code>retries</code> configuration will not"
            + " take effect (as the client won't generally know of any failures). The offset given back for each record will"
            + " always be set to -1."
            + " <li><code>acks=1</code> This will mean the leader will write the record to its local log but will respond"
            + " without awaiting full acknowledgement from all followers. In this case should the leader fail immediately after"
            + " acknowledging the record but before the followers have replicated it then the record will be lost."
            + " <li><code>acks=all</code> This means the leader will wait for the full set of in-sync replicas to"
            + " acknowledge the record. This guarantees that the record will not be lost as long as at least one in-sync replica"
            + " remains alive. This is the strongest available guarantee. This is equivalent to the acks=-1 setting.";

    /** <code>timeout.ms</code> */

    /**
     * @deprecated This config will be removed in a future release. Please use {@link #REQUEST_TIMEOUT_MS_CONFIG}
     */
    @Deprecated
    public static final String TIMEOUT_CONFIG = "timeout.ms";
    private static final String TIMEOUT_DOC = "The configuration controls the maximum amount of time the server will wait for acknowledgments from followers to "
            + "meet the acknowledgment requirements the producer has specified with the <code>acks</code> configuration. If the "
            + "requested number of acknowledgments are not met when the timeout elapses an error will be returned. This timeout "
            + "is measured on the server side and does not include the network latency of the request.";

    /**
     * <code>linger.ms</code>
     */
    public static final String LINGER_MS_CONFIG = "linger.ms";
    private static final String LINGER_MS_DOC = "The producer groups together any records that arrive in between request transmissions into a single batched request. "
            + "Normally this occurs only under load when records arrive faster than they can be sent out. However in some circumstances the client may want to "
            + "reduce the number of requests even under moderate load. This setting accomplishes this by adding a small amount "
            + "of artificial delay&mdash;that is, rather than immediately sending out a record the producer will wait for up to "
            + "the given delay to allow other records to be sent so that the sends can be batched together. This can be thought "
            + "of as analogous to Nagle's algorithm in TCP. This setting gives the upper bound on the delay for batching: once "
            + "we get <code>" + BATCH_SIZE_CONFIG + "</code> worth of records for a partition it will be sent immediately regardless of this "
            + "setting, however if we have fewer than this many bytes accumulated for this partition we will 'linger' for the "
            + "specified time waiting for more records to show up. This setting defaults to 0 (i.e. no delay). Setting <code>" + LINGER_MS_CONFIG + "=5</code>, "
            + "for example, would have the effect of reducing the number of requests sent but would add up to 5ms of latency to records sent in the absense of load.";

    /**
     * <code>client.id</code>
     */
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

    /**
     * <code>send.buffer.bytes</code>
     */
    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

    /**
     * <code>receive.buffer.bytes</code>
     */
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

    /**
     * <code>record.serializer.field_index</code>
     */
    public static final String RECORD_SERIALIZER_FIELD_OFFSET_CONFIG = "record.serializer.field_offset";
    private static final String RECORD_SERIALIZER_FIELD_OFFSET_DOC = "The configuration controls whether the data serializer should" +
            " serialize the record with field offsets. The default value is true.";

    /**
     * <code>max.request.size</code>
     */
    public static final String MAX_REQUEST_SIZE_CONFIG = "max.request.size";
    private static final String MAX_REQUEST_SIZE_DOC = "The maximum size of a request in bytes. This is also effectively a cap on the maximum record size. Note that the server "
            + "has its own cap on record size which may be different from this. This setting will limit the number of record "
            + "batches the producer will send in a single request to avoid sending huge requests.";

    /**
     * <code>reconnect.backoff.ms</code>
     */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /**
     * <code>max.block.ms</code>
     */
    public static final String MAX_BLOCK_MS_CONFIG = "max.block.ms";
    private static final String MAX_BLOCK_MS_DOC = "The configuration controls how long <code>KafkaProducer.send()</code> and <code>KafkaProducer.partitionsFor()</code> will block."
            + "These methods can be blocked either because the buffer is full or metadata unavailable."
            + "Blocking in the user-supplied serializers or partitioner will not be counted against this timeout.";

    /** <code>block.on.buffer.full</code> */
    /**
     * @deprecated This config will be removed in a future release. Please use {@link #MAX_BLOCK_MS_CONFIG}.
     */
    @Deprecated
    public static final String BLOCK_ON_BUFFER_FULL_CONFIG = "block.on.buffer.full";
    private static final String BLOCK_ON_BUFFER_FULL_DOC = "When our memory buffer is exhausted we must either stop accepting new records (block) or throw errors. "
            + "By default this setting is false and the producer will no longer throw a BufferExhaustException but instead will use the <code>" + MAX_BLOCK_MS_CONFIG + "</code> "
            + "value to block, after which it will throw a TimeoutException. Setting this property to true will set the <code>" + MAX_BLOCK_MS_CONFIG + "</code> to Long.MAX_VALUE. "
            + "<em>Also if this property is set to true, parameter <code>" + METADATA_FETCH_TIMEOUT_CONFIG + "</code> is no longer honored.</em>"
            + "<p>This parameter is deprecated and will be removed in a future release. "
            + "Parameter <code>" + MAX_BLOCK_MS_CONFIG + "</code> should be used instead.";

    /**
     * <code>buffer.memory</code>
     */
    public static final String BUFFER_MEMORY_CONFIG = "buffer.memory";
    private static final String BUFFER_MEMORY_DOC = "The total bytes of memory the producer can use to buffer records waiting to be sent to the server. If records are "
            + "sent faster than they can be delivered to the server the producer will block for <code>" + MAX_BLOCK_MS_CONFIG + "</code> after which it will throw an exception."
            + "<p>"
            + "This setting should correspond roughly to the total memory the producer will use, but is not a hard bound since "
            + "not all memory the producer uses is used for buffering. Some additional memory will be used for compression (if "
            + "compression is enabled) as well as for maintaining in-flight requests.";

    /**
     * <code>retry.backoff.ms</code>
     */
    public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;

    /**
     * <code>compression.type</code>
     */
    public static final String COMPRESSION_TYPE_CONFIG = "compression.type";
    private static final String COMPRESSION_TYPE_DOC = "The compression type for all data generated by the producer. The default is none (i.e. no compression). Valid "
            + " values are <code>none</code>, <code>gzip</code>, <code>snappy</code>, or <code>lz4</code>. "
            + "Compression is of full batches of data, so the efficacy of batching will also impact the compression ratio (more batching means better compression).";

    /**
     * <code>metrics.sample.window.ms</code>
     */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;

    /**
     * <code>metrics.num.samples</code>
     */
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;

    /**
     * <code>metric.reporters</code>
     */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /**
     * <code>max.in.flight.requests.per.connection</code>
     */
    public static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "max.in.flight.requests.per.connection";
    private static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC = "The maximum number of unacknowledged requests the client will send on a single connection before blocking."
            + " Note that if this setting is set to be greater than 1 and there are failed sends, there is a risk of"
            + " message re-ordering due to retries (i.e., if retries are enabled).";

    /**
     * <code>retries</code>
     */
    public static final String RETRIES_CONFIG = "retries";
    private static final String RETRIES_DOC = "Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error."
            + " Note that this retry is no different than if the client resent the record upon receiving the error."
            + " Allowing retries without setting <code>" + MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + "</code> to 1 will potentially change the"
            + " ordering of records because if two batches are sent to a single partition, and the first fails and is retried but the second"
            + " succeeds, then the records in the second batch may appear first.";

    /**
     * <code>connections.max.idle.ms</code>
     */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /**
     * <code>partitioner.class</code>
     */
    public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
    private static final String PARTITIONER_CLASS_DOC = "Partitioner class that implements the <code>Partitioner</code> interface.";

    /**
     * <code>request.timeout.ms</code>
     */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC
            + " This should be larger than replica.lag.time.max.ms (a broker configuration)"
            + " to reduce the possibility of message duplication due to unnecessary producer retries.";

    /**
     * <code>interceptor.classes</code>
     */
    public static final String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
    public static final String INTERCEPTOR_CLASSES_DOC = "A list of classes to use as interceptors. "
            + "Implementing the <code>ProducerInterceptor</code> interface allows you to intercept (and possibly mutate) the records "
            + "received by the producer before they are published to the Kafka cluster. By default, there are no interceptors.";

    static {
        CONFIG = new ConfigDef().define(BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                .define(BUFFER_MEMORY_CONFIG, ConfigDef.Type.LONG, 32 * 1024 * 1024L, atLeast(0L), ConfigDef.Importance.HIGH, BUFFER_MEMORY_DOC)
                .define(RETRIES_CONFIG, ConfigDef.Type.INT, 0, between(0, Integer.MAX_VALUE), ConfigDef.Importance.HIGH, RETRIES_DOC)
                .define(ACKS_CONFIG,
                        ConfigDef.Type.STRING,
                        "1",
                        in("all", "-1", "0", "1"),
                        ConfigDef.Importance.HIGH,
                        ACKS_DOC)
                .define(COMPRESSION_TYPE_CONFIG, ConfigDef.Type.STRING, "none", ConfigDef.Importance.HIGH, COMPRESSION_TYPE_DOC)
                .define(BATCH_SIZE_CONFIG, ConfigDef.Type.INT, 16384, atLeast(0), ConfigDef.Importance.MEDIUM, BATCH_SIZE_DOC)
                .define(TIMEOUT_CONFIG, ConfigDef.Type.INT, 30 * 1000, atLeast(0), ConfigDef.Importance.MEDIUM, TIMEOUT_DOC)
                .define(LINGER_MS_CONFIG, ConfigDef.Type.LONG, 0, atLeast(0L), ConfigDef.Importance.MEDIUM, LINGER_MS_DOC)
                .define(CLIENT_ID_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, CommonClientConfigs.CLIENT_ID_DOC)
                .define(SEND_BUFFER_CONFIG, ConfigDef.Type.INT, 128 * 1024, atLeast(-1), ConfigDef.Importance.MEDIUM, CommonClientConfigs.SEND_BUFFER_DOC)
                .define(RECEIVE_BUFFER_CONFIG, ConfigDef.Type.INT, 32 * 1024, atLeast(-1), ConfigDef.Importance.MEDIUM, CommonClientConfigs.RECEIVE_BUFFER_DOC)
                .define(MAX_REQUEST_SIZE_CONFIG,
                        ConfigDef.Type.INT,
                        1 * 1024 * 1024,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        MAX_REQUEST_SIZE_DOC)
                .define(BLOCK_ON_BUFFER_FULL_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, BLOCK_ON_BUFFER_FULL_DOC)
                .define(RECONNECT_BACKOFF_MS_CONFIG, ConfigDef.Type.LONG, 50L, atLeast(0L), ConfigDef.Importance.LOW, CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                .define(METRIC_REPORTER_CLASSES_CONFIG, ConfigDef.Type.LIST, "", ConfigDef.Importance.LOW, CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                .define(RETRY_BACKOFF_MS_CONFIG, ConfigDef.Type.LONG, 100L, atLeast(0L), ConfigDef.Importance.LOW, CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                .define(METADATA_FETCH_TIMEOUT_CONFIG,
                        ConfigDef.Type.LONG,
                        60 * 1000,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        METADATA_FETCH_TIMEOUT_DOC)
                .define(MAX_BLOCK_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        60 * 1000,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        MAX_BLOCK_MS_DOC)
                .define(REQUEST_TIMEOUT_MS_CONFIG,
                        ConfigDef.Type.INT,
                        30 * 1000,
                        atLeast(0),
                        ConfigDef.Importance.MEDIUM,
                        REQUEST_TIMEOUT_MS_DOC)
                .define(METADATA_MAX_AGE_CONFIG, ConfigDef.Type.LONG, 5 * 60 * 1000, atLeast(0), ConfigDef.Importance.LOW, METADATA_MAX_AGE_DOC)
                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        30000,
                        atLeast(0),
                        ConfigDef.Importance.LOW,
                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                .define(METRICS_NUM_SAMPLES_CONFIG, ConfigDef.Type.INT, 2, atLeast(1), ConfigDef.Importance.LOW, CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                .define(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                        ConfigDef.Type.INT,
                        5,
                        atLeast(1),
                        ConfigDef.Importance.LOW,
                        MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_DOC)
                                /* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        9 * 60 * 1000,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                .define(PARTITIONER_CLASS_CONFIG,
                        ConfigDef.Type.CLASS,
                        DefaultPartitioner.class,
                        ConfigDef.Importance.MEDIUM, PARTITIONER_CLASS_DOC)
                .define(INTERCEPTOR_CLASSES_CONFIG,
                        ConfigDef.Type.LIST,
                        null,
                        ConfigDef.Importance.LOW,
                        INTERCEPTOR_CLASSES_DOC)
                .define(RECORD_SERIALIZER_FIELD_OFFSET_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.LOW,
                        RECORD_SERIALIZER_FIELD_OFFSET_DOC)

                // security support
                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        ConfigDef.Type.STRING,
                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                        ConfigDef.Importance.MEDIUM,
                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                .withClientSslSupport()
                .withClientSaslSupport();

    }

    SchemafulProducerConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    SchemafulProducerConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }
}
