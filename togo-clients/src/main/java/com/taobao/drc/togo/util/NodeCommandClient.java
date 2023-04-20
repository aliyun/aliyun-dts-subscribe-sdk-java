package com.taobao.drc.togo.util;

/**
 * Created by longxuan on 18/2/24.
 */
import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by longxuan on 17/11/8.
 */
public class NodeCommandClient {

    public static final ConfigDef DEFAULT_CLIENT_CONFIG = NodeCommandClientConfig.configDef()
            .withClientSaslSupport()
            .withClientSslSupport();

    public static class CommandClientConfig extends AbstractConfig {
        public static CommandClientConfig fromNodeCommandClientConfig(NodeCommandClientConfig NodeCommandClientConfig) {
            return new CommandClientConfig(NodeCommandClientConfig.originals());
        }
        public CommandClientConfig(Map<?, ?> originals) {
            super(DEFAULT_CLIENT_CONFIG, originals);
        }
    }

    public static final class CommandClientProviderException extends RuntimeException {
        public CommandClientProviderException(String message, Exception e) {
            super(message, e);
        }
        public CommandClientProviderException(String message) {
            super(message);
        }
    }

    public static final class CommandClient {
        private final KafkaClient kafkaClient;
        private final CommandClientConfig commandClientConfig;
        private final List<MetricsReporter> reporters;
        private static final int MAX_INFLIGHT_REQUESTS = 100;
        public CommandClient(CommandClientConfig config) {
            commandClientConfig = config;
            final Time time = new SystemTime();

            final Map<String, String> metricTags = new LinkedHashMap<>();
            final String clientId = commandClientConfig.getString(NodeCommandClientConfig.CLIENT_ID_CONFIG);
            metricTags.put("client-id", clientId);
            final Metadata metadata = new Metadata(commandClientConfig.getLong(
                    NodeCommandClientConfig.RETRY_BACKOFF_MS_CONFIG),
                    commandClientConfig.getLong(NodeCommandClientConfig.METADATA_MAX_AGE_CONFIG), false
            );
            final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(commandClientConfig.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
            metadata.update(Cluster.bootstrap(addresses), Collections.EMPTY_SET, time.milliseconds());

            final MetricConfig metricConfig = new MetricConfig().samples(commandClientConfig.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(commandClientConfig.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .tags(metricTags);
            reporters = commandClientConfig.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    MetricsReporter.class);
            // TODO: This should come from the KafkaStream
            reporters.add(new JmxReporter("kafka.admin.client"));
            final Metrics metrics = new Metrics(metricConfig, reporters, time);

            final ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(commandClientConfig);
            final LogContext logContext = createLogContext(clientId);



            final Selector selector = new Selector(
                    commandClientConfig.getLong(NodeCommandClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                    metrics,
                    time,
                    "kafka-client",
                    channelBuilder,
                    logContext);

            kafkaClient = new NetworkClient(
                    selector,
                    metadata,
                    clientId,
                    MAX_INFLIGHT_REQUESTS, // a fixed large enough value will suffice
                    commandClientConfig.getLong(NodeCommandClientConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    commandClientConfig.getLong(NodeCommandClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                    commandClientConfig.getInt(NodeCommandClientConfig.SEND_BUFFER_CONFIG),
                    commandClientConfig.getInt(NodeCommandClientConfig.RECEIVE_BUFFER_CONFIG),
                    commandClientConfig.getInt(NodeCommandClientConfig.REQUEST_TIMEOUT_MS_CONFIG),
                    time,
                    true,
                    new ApiVersions(),
                    logContext);
        }

        public void close() {
            if (null != kafkaClient) {
                try {
                    kafkaClient.close();
                } catch (Exception e) {
                }
            }
        }

        private Node ensureOneNodeIsReady(final List<Node> nodes) {
            Node brokerId = null;
            final long readyTimeout = Time.SYSTEM.milliseconds() + commandClientConfig.getInt(NodeCommandClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
            boolean foundNode = false;
            while (!foundNode && (Time.SYSTEM.milliseconds() < readyTimeout)) {
                for (Node node: nodes) {
                    if (kafkaClient.ready(node, Time.SYSTEM.milliseconds())) {
                        brokerId = node;
                        foundNode = true;
                        break;
                    }
                }
                try {
                    kafkaClient.poll(commandClientConfig.getLong(NodeCommandClientConfig.POLL_MS_CONFIG), Time.SYSTEM.milliseconds());
                } catch (final Exception e) {
                    throw new CommandClientProviderException("Could not poll.", e);
                }
            }
            if (brokerId == null) {
                throw new CommandClientProviderException("Could not find any available broker. " +
                        "Check your NodeCommandClientConfig setting '" + NodeCommandClientConfig.BOOTSTRAP_SERVERS_CONFIG + "'. " +
                        "This error might also occur, if you try to connect to pre-0.10 brokers. " +
                        "Kafka Streams requires broker version 0.10.1.x or higher.");
            }
            return brokerId;
        }

        private ClientResponse sendRequestSync(final ClientRequest clientRequest) {
            try {
                kafkaClient.send(clientRequest, Time.SYSTEM.milliseconds());
            } catch (final Exception e) {
                throw new CommandClientProviderException("Could not send request.", e);
            }
            final long responseTimeout = Time.SYSTEM.milliseconds() + commandClientConfig.getInt(NodeCommandClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
            // Poll for the response.
            while (Time.SYSTEM.milliseconds() < responseTimeout) {
                final List<ClientResponse> responseList;
                try {
                    responseList = kafkaClient.poll(commandClientConfig.getLong(NodeCommandClientConfig.POLL_MS_CONFIG), Time.SYSTEM.milliseconds());
                } catch (final IllegalStateException e) {
                    throw new CommandClientProviderException("Could not poll.", e);
                }
                if (!responseList.isEmpty()) {
                    if (responseList.size() > 1) {
                        throw new CommandClientProviderException("Sent one request but received multiple or no responses.");
                    }
                    ClientResponse response = responseList.get(0);
                    if (response.requestHeader().correlationId() == clientRequest.correlationId()) {
                        return response;
                    } else {
                        throw new CommandClientProviderException("Inconsistent response received from the broker "
                                + clientRequest.destination() + ", expected correlation id " + clientRequest.correlationId()
                                + ", but received " + response.requestHeader().correlationId());
                    }
                }
            }
            throw new CommandClientProviderException("Failed to get response from broker within timeout");

        }

        private <T extends AbstractResponse> T doRequest(Node destination, AbstractRequest.Builder builder,
                                                         Class type, String typeName) {
            ensureOneNodeIsReady(Arrays.asList(destination));
            if (null == destination || destination.isEmpty()) {
                throw new CommandClientProviderException("destination is required");
            }
            final ClientRequest clientRequest = kafkaClient.newClientRequest(
                    destination.idString(),
                    builder,
                    Time.SYSTEM.milliseconds(),
                    true);
            final ClientResponse clientResponse = sendRequestSync(clientRequest);

            if (!clientResponse.hasResponse()) {
                throw new CommandClientProviderException("Empty response for client request.");
            }
            if (!type.isAssignableFrom(clientResponse.responseBody().getClass())) {
                throw new CommandClientProviderException("Inconsistent response type for internal topic  " + typeName + "Request. " +
                        "Expected " + typeName + "Response but received " + clientResponse.responseBody().getClass().getName());
            }
            return (T)(clientResponse.responseBody());

        }

        public MetadataResponse fetchMetadata() {
            return doRequest(getAnyReadyBrokerId(), MetadataRequest.Builder.allTopics(),
                    MetadataResponse.class,
                    "MetaData");
        }

        private static final AtomicInteger incrementCounter = new AtomicInteger(100);

        public ReloadAuthMetaResponse reloadAuthMeta(String host, int port, ReloadAuthMetaRequest.DisconnectStrategy strategy, List<String> disconnectList) {
            return reloadAuthMeta(new Node(incrementCounter.getAndIncrement(), host, port), strategy, disconnectList);
        }

        public ReloadAuthMetaResponse reloadAuthMeta(Node destination, ReloadAuthMetaRequest.DisconnectStrategy strategy, List<String> disconnectList) {
            return doRequest(destination,
                    new ReloadAuthMetaRequest.Builder(-1, strategy, disconnectList),
                    ReloadAuthMetaResponse.class,
                    "ReloadAuthResponse");
        }

        private Node getAnyReadyBrokerId() {
            final Metadata metadata = new Metadata(
                    commandClientConfig.getLong(NodeCommandClientConfig.RETRY_BACKOFF_MS_CONFIG),
                    commandClientConfig.getLong(NodeCommandClientConfig.METADATA_MAX_AGE_CONFIG),
                    false);
            final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(commandClientConfig.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
            metadata.update(Cluster.bootstrap(addresses), Collections.EMPTY_SET, Time.SYSTEM.milliseconds());

            final List<Node> nodes = metadata.fetch().nodes();
            return ensureOneNodeIsReady(nodes);
        }

        public void checkBrokerCompatibility(final boolean eosEnabled)  {
            final ClientRequest clientRequest = kafkaClient.newClientRequest(
                    getAnyReadyBrokerId().idString(),
                    new ApiVersionsRequest.Builder(),
                    Time.SYSTEM.milliseconds(),
                    true);

            final ClientResponse clientResponse = sendRequestSync(clientRequest);
            if (!clientResponse.hasResponse()) {
                throw new CommandClientProviderException("Empty response for client request.");
            }
            if (!(clientResponse.responseBody() instanceof ApiVersionsResponse)) {
                throw new CommandClientProviderException("Inconsistent response type for API versions request. " +
                        "Expected ApiVersionsResponse but received " + clientResponse.responseBody().getClass().getName());
            }

            final ApiVersionsResponse apiVersionsResponse =  (ApiVersionsResponse) clientResponse.responseBody();

            if (apiVersionsResponse.apiVersion(ApiKeys.CREATE_TOPICS.id) == null) {
                throw new CommandClientProviderException("Kafka Streams requires broker version 0.10.1.x or higher.");
            }
            if (eosEnabled && !brokerSupportsTransactions(apiVersionsResponse)) {
                throw new CommandClientProviderException("Setting  processing.guarantee exactly_once requires broker version 0.11.0.x or higher.");
            }
        }

        private boolean brokerSupportsTransactions(final ApiVersionsResponse apiVersionsResponse) {
            return apiVersionsResponse.apiVersion(ApiKeys.INIT_PRODUCER_ID.id) != null
                    && apiVersionsResponse.apiVersion(ApiKeys.ADD_PARTITIONS_TO_TXN.id) != null
                    && apiVersionsResponse.apiVersion(ApiKeys.ADD_OFFSETS_TO_TXN.id) != null
                    && apiVersionsResponse.apiVersion(ApiKeys.END_TXN.id) != null
                    && apiVersionsResponse.apiVersion(ApiKeys.WRITE_TXN_MARKERS.id) != null
                    && apiVersionsResponse.apiVersion(ApiKeys.TXN_OFFSET_COMMIT.id) != null;
        }
    }



    public static MetadataResponse fetchMetaDataFromHost(String host) {
        final Properties props = new Properties();
        props.setProperty(NodeCommandClientConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        final NodeCommandClientConfig NodeCommandClientConfig = new NodeCommandClientConfig(props);
        final CommandClientConfig config = CommandClientConfig.fromNodeCommandClientConfig(NodeCommandClientConfig);
        CommandClient proxyClient = new CommandClient(config);
        //       proxyClient.checkBrokerCompatibility();

        MetadataResponse metadataResponse = proxyClient.fetchMetadata();
        return metadataResponse;
    }

    private static LogContext createLogContext(String clientId) {
        return new LogContext("[NodeCommandClient clientId=" + clientId + "] ");
    }
    /*
    public static void main(String args[]) {
        final Properties props = new Properties();
        props.setProperty(NodeCommandClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:42222");
        final NodeCommandClientConfig NodeCommandClientConfig = new NodeCommandClientConfig(props);
        final CommandClientConfig config = CommandClientConfig.fromNodeCommandClientConfig(NodeCommandClientConfig);
        CommandClient proxyClient = new CommandClient(config);
        //       proxyClient.checkBrokerCompatibility();
        long beginTime = System.currentTimeMillis();
        MetadataResponse metadataResponse = proxyClient.fetchMetadata();
        long endTime = System.currentTimeMillis();
        System.out.println(metadataResponse);
        System.out.println("Fetch metadata cost " + (endTime - beginTime) + " ms");

    }
    */
}

