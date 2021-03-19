package com.aliyun.dts.subscribe.clients.check.util;

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
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    }

    private static LogContext createLogContext(String clientId) {
        return new LogContext("[NodeCommandClient clientId=" + clientId + "] ");
    }
}
