package com.taobao.drc.togo.common.network;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.metrics.Sensor;

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

public class ProxyNetworkClientConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;


    /**
     * <code>client.id</code>
     */
    public static final String CLIENT_ID_CONFIG = CommonClientConfigs.CLIENT_ID_CONFIG;

    /** <code>connections.max.idle.ms</code> */
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG;

    /**
     * <code>metrics.num.samples</code>
     */
    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;
    /**
     * <code>metrics.sample.window.ms</code>
     */
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;
    /**
     * <code>metrics.log.level</code>
     */
    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;

    /**
     * <code>metric.reporters</code>
     */
    public static final String METRIC_REPORTER_CLASSES_CONFIG = CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG;

    /**
     * <code>reconnect.backoff.ms</code>
     */
    public static final String RECONNECT_BACKOFF_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG;

    /**
     * <code>reconnect.backoff.max.ms</code>
     */
    public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG;

    /** <code>send.buffer.bytes</code> */
    public static final String SEND_BUFFER_CONFIG = CommonClientConfigs.SEND_BUFFER_CONFIG;

    /** <code>receive.buffer.bytes</code> */
    public static final String RECEIVE_BUFFER_CONFIG = CommonClientConfigs.RECEIVE_BUFFER_CONFIG;

    /** <code>request.timeout.ms</code> */
    public static final String REQUEST_TIMEOUT_MS_CONFIG = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
    private static final String REQUEST_TIMEOUT_MS_DOC = CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC;

    static {
        CONFIG = new ConfigDef()
                .define(CLIENT_ID_CONFIG,
                        Type.STRING,
                        "proxyclient",
                        Importance.LOW,
                        CommonClientConfigs.CLIENT_ID_DOC)
                /* default is set to be a bit lower than the server default (10 min), to avoid both client and server closing connection at same time */
                .define(CONNECTIONS_MAX_IDLE_MS_CONFIG,
                        Type.LONG,
                        9 * 60 * 1000,
                        Importance.MEDIUM,
                        CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
                .define(METRICS_NUM_SAMPLES_CONFIG,
                        Type.INT,
                        2,
                        atLeast(1),
                        Importance.LOW,
                        CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
                .define(METRICS_SAMPLE_WINDOW_MS_CONFIG,
                        Type.LONG,
                        30000,
                        atLeast(0),
                        Importance.LOW,
                        CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
                .define(METRICS_RECORDING_LEVEL_CONFIG,
                        Type.STRING,
                        Sensor.RecordingLevel.INFO.toString(),
                        in(Sensor.RecordingLevel.INFO.toString(), Sensor.RecordingLevel.DEBUG.toString()),
                        Importance.LOW,
                        CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC)
                .define(METRIC_REPORTER_CLASSES_CONFIG,
                        Type.LIST,
                        "",
                        Importance.LOW,
                        CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC)
                .define(RECONNECT_BACKOFF_MS_CONFIG,
                        Type.LONG,
                        50L,
                        atLeast(0L),
                        Importance.LOW,
                        CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
                .define(RECONNECT_BACKOFF_MAX_MS_CONFIG,
                        Type.LONG,
                        1000L,
                        atLeast(0L),
                        Importance.LOW,
                        CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
                .define(SEND_BUFFER_CONFIG,
                        Type.INT,
                        128 * 1024,
                        atLeast(-1),
                        Importance.MEDIUM,
                        CommonClientConfigs.SEND_BUFFER_DOC)
                .define(RECEIVE_BUFFER_CONFIG,
                        Type.INT,
                        64 * 1024,
                        atLeast(-1),
                        Importance.MEDIUM,
                        CommonClientConfigs.RECEIVE_BUFFER_DOC)
                .define(REQUEST_TIMEOUT_MS_CONFIG,
                        Type.INT,
                        305000, // chosen to be higher than the default of max.poll.interval.ms
                        atLeast(0),
                        Importance.MEDIUM,
                        REQUEST_TIMEOUT_MS_DOC)
                // security support
                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        Type.STRING,
                        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                        Importance.MEDIUM,
                        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                .withClientSslSupport()
                .withClientSaslSupport();

    }


    public ProxyNetworkClientConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    public ProxyNetworkClientConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

}
