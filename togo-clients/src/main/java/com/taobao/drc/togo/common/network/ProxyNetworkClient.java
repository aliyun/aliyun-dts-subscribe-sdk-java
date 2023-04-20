package com.taobao.drc.togo.common.network;

import com.taobao.drc.togo.util.SIDUtil;
import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.clients.consumer.internals.RequestFutureListener;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.network.*;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class ProxyNetworkClient {
    private static final Logger log = LoggerFactory.getLogger(ProxyNetworkClient.class);
    private static final String JMX_PREFIX = "proxy.consumer";

    private ProxyNetworkClientConfig config;
    final Metrics metrics;
    private final Time time;
    private ConsumerNetworkClient consumerNetworkClient;

    private Thread pollThread;
    private AtomicBoolean started = new AtomicBoolean(false);
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    private BlockingQueue<UnsendRequest> unSendQueue;

    public ProxyNetworkClient(ProxyNetworkClientConfig config) {
        this.config = config;
        this.unSendQueue = new LinkedBlockingQueue<>(100);

        this.time = Time.SYSTEM;
        String clientId = this.config.getString(ProxyNetworkClientConfig.CLIENT_ID_CONFIG);
        Map<String, String> metricsTags = Collections.singletonMap("client-id", clientId);
        MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProxyNetworkClientConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(ProxyNetworkClientConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .recordLevel(Sensor.RecordingLevel.forName(config.getString(ProxyNetworkClientConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                .tags(metricsTags);
        List<MetricsReporter> reporters = config.getConfiguredInstances(ProxyNetworkClientConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class);
        reporters.add(new JmxReporter(JMX_PREFIX));
        this.metrics = new Metrics(metricConfig, reporters, time);

        String metricGrpPrefix = "proxyConsumer";
        LogContext logContext = new LogContext("[ProxyConsumer clientId=" + clientId + "] ");



        ManualMetadataUpdater metadataUpdate = new ManualMetadataUpdater();
//        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config);
        ChannelBuilder channelBuilder = createChannelBuilder(config);
        NetworkClient netClient = new NetworkClient(
                new Selector(config.getLong(ProxyNetworkClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), metrics, time, metricGrpPrefix, channelBuilder, logContext),
                metadataUpdate,
                clientId,
                100, // a fixed large enough value will suffice for max in-flight requests
                config.getLong(ProxyNetworkClientConfig.RECONNECT_BACKOFF_MS_CONFIG),
                config.getLong(ProxyNetworkClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                config.getInt(ProxyNetworkClientConfig.SEND_BUFFER_CONFIG),
                config.getInt(ProxyNetworkClientConfig.RECEIVE_BUFFER_CONFIG),
                config.getInt(ProxyNetworkClientConfig.REQUEST_TIMEOUT_MS_CONFIG),
                time,
                false,
                new ApiVersions(),
                logContext);

        Metadata metadata = new Metadata(Long.MAX_VALUE, Long.MAX_VALUE, false);
        this.consumerNetworkClient = new ConsumerNetworkClient(logContext, netClient, metadata, time, Long.MAX_VALUE, Long.MAX_VALUE);
        this.consumerNetworkClient.disableWakeups();
    }


    public RequestFuture<ClientResponse> send(Node node, AbstractRequest.Builder<?> requestBuilder) {
        UnsendRequest unsendRequest = new UnsendRequest(node, requestBuilder);
        try {
            unSendQueue.put(unsendRequest);
        } catch (InterruptedException e) {
            unsendRequest.responstFuture.raise(new RuntimeException(e.getMessage(), e));
        }
        consumerNetworkClient.wakeup();
        return unsendRequest.responstFuture;
    }

    public void disconnect(Node node) {
        consumerNetworkClient.disconnect(node);
    }

    public void startup() {
        if (started.compareAndSet(false, true)) {
            pollThread = new Thread(this::run, "ProxyConsumerNetworkClient-poll-thread");
            pollThread.start();
            log.info("ProxyConsumerNetworkClient start");
        }
    }

    public void shutdown() {
        if (started.get() && isRunning.get()) {
            isRunning.set(false);
            try {
                pollThread.join(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
            }
            log.info("shut down ProxyNetworkClient success");
        }
    }

    private void run() {
        log.info("Thread:[{}] startd.", Thread.currentThread().getName());
        isRunning.set(true);
        while (isRunning.get()) {
            try {
                UnsendRequest unsendRequest = unSendQueue.poll(10, TimeUnit.MILLISECONDS);
                if (unsendRequest != null) {
                    consumerNetworkClient.send(unsendRequest.node, unsendRequest.requestBuilder).addListener(new RequestFutureListener<ClientResponse>() {
                        @Override
                        public void onSuccess(ClientResponse value) {
                            synchronized (unsendRequest.responstFuture) {
                                if (!unsendRequest.responstFuture.isDone()) {
                                    unsendRequest.responstFuture.complete(value);
                                }
                            }
                        }

                        @Override
                        public void onFailure(RuntimeException e) {
                            synchronized (unsendRequest.responstFuture) {
                                if (!unsendRequest.responstFuture.isDone()) {
                                    unsendRequest.responstFuture.raise(e);
                                }
                            }
                        }
                    });
                }
                consumerNetworkClient.poll(10);
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
            }
        }
        log.info("Thread:[{}] exist.", Thread.currentThread().getName());
    }


    public static ChannelBuilder createChannelBuilder(AbstractConfig config) {
        SecurityProtocol securityProtocol = SecurityProtocol.forName(config.getString(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        String clientSaslMechanism = config.getString(SaslConfigs.SASL_MECHANISM);
        JaasContext jaasContext = JaasContext.load(JaasContext.Type.CLIENT, null, config.values());
        ChannelBuilder channelBuilder = new ProxyChannelBuilder(Mode.CLIENT, jaasContext, securityProtocol, null,
                clientSaslMechanism, true, null);
        channelBuilder.configure(config.values());
        return channelBuilder;
    }

    private static class ProxyChannelBuilder extends SaslChannelBuilder {
        private final String clientSaslMechanism;
        private final boolean handshakeRequestEnable;

        public ProxyChannelBuilder(Mode mode, JaasContext jaasContext, SecurityProtocol securityProtocol, ListenerName listenerName, String clientSaslMechanism, boolean handshakeRequestEnable, CredentialCache credentialCache) {
            super(mode, jaasContext, securityProtocol, listenerName, clientSaslMechanism, handshakeRequestEnable, credentialCache);
            this.clientSaslMechanism = clientSaslMechanism;
            this.handshakeRequestEnable = handshakeRequestEnable;
        }

        @Override
        protected SaslClientAuthenticator buildClientAuthenticator(Map<String, ?> configs, String id,
                                                                   String serverHost, String servicePrincipal, TransportLayer transportLayer, Subject subject) throws IOException {
            String userName = SIDUtil.getUserNameFromString(id);
            String sid = SIDUtil.getSIDFromString(id);
            String password = SIDUtil.getPasswordFromString(id);

            Subject subject1 = new Subject(false, Collections.EMPTY_SET, Collections.singleton(userName+ SIDUtil.SIDSplitChar+sid), Collections.singleton(password));
            return new SaslClientAuthenticator(configs, id, subject1, servicePrincipal,
                    serverHost, clientSaslMechanism, handshakeRequestEnable, transportLayer);
        }
    }

    private static class UnsendRequest {
        public Node node;
        public AbstractRequest.Builder<?> requestBuilder;
        public RequestFuture<ClientResponse> responstFuture;

        public UnsendRequest(Node node, AbstractRequest.Builder<?>  requestBuilder) {
            this.node = node;
            this.requestBuilder = requestBuilder;
            this.responstFuture = new RequestFuture<>();
        }
    }
}
